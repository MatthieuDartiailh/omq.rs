// Safety: all `UnsafeCell` dereferences in this module (`direct_recv_io`,
// `inproc_recv`, `recv_cache`) rely on compio's cooperative single-threaded
// runtime. Every access happens on the runtime thread that created the
// socket. The `Socket` API contract requires no concurrent send/recv on
// the same handle, so there is no data race.

use std::sync::{Arc, atomic::Ordering};

use bytes::Bytes;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::{Event, SocketType};
use omq_proto::routing::{RecvCategory, recv_category};

use crate::transport::inproc::InboundFrame;
use crate::transport::peer_io::PeerIo;

use super::Socket;
use super::inner::DirectIoState;

enum RecvAction {
    Return(Option<Message>),
    Retry,
    Proceed,
}

#[inline]
fn post_recv_needs_type_state(t: SocketType) -> bool {
    matches!(t, SocketType::Req | SocketType::Rep | SocketType::Dish)
}

fn is_identity_recv(t: SocketType) -> bool {
    recv_category(t) == RecvCategory::Identity
}

#[inline]
fn direct_recv_eligible(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Pull | SocketType::Sub | SocketType::Pair | SocketType::Req
    )
}

enum PullOutcome {
    Fed,
    Eof,
    ProtoErr,
    Err(std::io::Error),
    StartAccumulation,
}

impl From<crate::socket::OneShotLargeRecvOutcome> for PullOutcome {
    fn from(o: crate::socket::OneShotLargeRecvOutcome) -> Self {
        match o {
            crate::socket::OneShotLargeRecvOutcome::Skipped
            | crate::socket::OneShotLargeRecvOutcome::RearmMultiShot
            | crate::socket::OneShotLargeRecvOutcome::Took => Self::Fed,
            crate::socket::OneShotLargeRecvOutcome::AccumulatePayload => Self::StartAccumulation,
            crate::socket::OneShotLargeRecvOutcome::IoErr(e) => Self::Err(e),
            crate::socket::OneShotLargeRecvOutcome::ProtoErr(_) => Self::ProtoErr,
        }
    }
}

struct ClaimGuard<'a> {
    state: &'a Arc<DirectIoState>,
}

impl Drop for ClaimGuard<'_> {
    fn drop(&mut self) {
        self.state.recv_claim.store(0, Ordering::Release);
        self.state.recv_state_changed.notify(usize::MAX);
    }
}

async fn accumulate_large_recv(state: &Arc<DirectIoState>) -> Result<RecvAction> {
    while state.large_recv_pending.load(Ordering::Acquire) != 0 {
        let payload_len = state.large_recv_pending.load(Ordering::Acquire);

        let is_one_shot = {
            let sg = state.recv_stream.0.lock().await;
            matches!(sg.as_ref(), Some(crate::socket::RecvStreamState::OneShot))
        };

        if is_one_shot {
            let fd = state.lock_io().reader.fd_clone();
            let mut restore = crate::socket::AccRestore {
                state,
                buf: state.pending_acc.lock().expect("pending_acc").take(),
            };
            let acc = restore.buf.as_mut().expect("pending_acc missing");
            if let Err(_e) = fd.read_until(acc, payload_len).await {
                state.signal_eof();
                return Err(Error::Closed);
            }
            state.last_input_nanos.store(
                state.hb_epoch.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
            let payload = restore.buf.take().unwrap().freeze();
            state.large_recv_pending.store(0, Ordering::Release);
            let mut io = state.lock_io();
            if let Err(_e) = io.codec.supply_payload(payload) {
                state.signal_eof();
                return Ok(RecvAction::Return(None));
            }
            continue;
        }

        let stream_result = {
            let mut sguard = state.recv_stream.0.lock().await;
            let Some(crate::socket::RecvStreamState::MultiShot(cs)) = sguard.as_mut() else {
                state.signal_eof();
                return Err(Error::Closed);
            };
            compio::runtime::FutureExt::with_cancel(
                futures::StreamExt::next(&mut cs.stream),
                cs.cancel.clone(),
            )
            .await
        };
        match stream_result {
            Some(Ok(buf)) if !buf.is_empty() => {
                let mut acc_guard = state.pending_acc.lock().expect("pending_acc");
                let acc = acc_guard.as_mut().expect("pending_acc missing");
                let needed = payload_len - acc.len();
                let extra = if buf.len() <= needed {
                    acc.extend_from_slice(&buf[..]);
                    drop(buf);
                    None
                } else {
                    acc.extend_from_slice(&buf[..needed]);
                    let e = Bytes::copy_from_slice(&buf[needed..]);
                    drop(buf);
                    Some(e)
                };
                state.last_input_nanos.store(
                    state.hb_epoch.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                if acc.len() >= payload_len {
                    let payload = acc_guard.take().unwrap().freeze();
                    drop(acc_guard);
                    state.large_recv_pending.store(0, Ordering::Release);
                    let mut io = state.lock_io();
                    if let Err(_e) = io.codec.supply_payload(payload) {
                        state.signal_eof();
                        return Ok(RecvAction::Return(None));
                    }
                    if let Some(extra) = extra
                        && let Err(_e) = io.codec.handle_input(extra)
                    {
                        state.signal_eof();
                        return Ok(RecvAction::Return(None));
                    }
                }
            }
            Some(Err(e))
                if e.raw_os_error() == Some(libc::ENOBUFS)
                    || e.raw_os_error() == Some(libc::ECANCELED) =>
            {
                let mut sguard = state.recv_stream.0.lock().await;
                *sguard = Some(crate::socket::RecvStreamState::OneShot);
            }
            None => {
                let mut sguard = state.recv_stream.0.lock().await;
                *sguard = Some(crate::socket::RecvStreamState::OneShot);
            }
            _ => {
                state.signal_eof();
                return Err(Error::Closed);
            }
        }
    }
    Ok(RecvAction::Proceed)
}

async fn pull_and_feed(state: &Arc<DirectIoState>) -> PullOutcome {
    let mut sguard = state.recv_stream.0.lock().await;
    match sguard.as_mut() {
        None => PullOutcome::Eof,
        Some(crate::socket::RecvStreamState::OneShot) => {
            crate::socket::one_shot_recv_and_feed(state, &mut sguard)
                .await
                .into()
        }
        Some(crate::socket::RecvStreamState::MultiShot(cs)) => {
            let buf = compio::runtime::FutureExt::with_cancel(
                futures::StreamExt::next(&mut cs.stream),
                cs.cancel.clone(),
            )
            .await;
            match buf {
                None => {
                    *sguard = Some(crate::socket::RecvStreamState::OneShot);
                    PullOutcome::Fed
                }
                Some(Err(e)) => {
                    let os = e.raw_os_error();
                    if os == Some(libc::ENOBUFS) || os == Some(libc::ECANCELED) {
                        *sguard = Some(crate::socket::RecvStreamState::OneShot);
                        PullOutcome::Fed
                    } else {
                        PullOutcome::Err(e)
                    }
                }
                Some(Ok(buf)) if buf.is_empty() => PullOutcome::Eof,
                Some(Ok(buf)) => {
                    let handle_result = {
                        let mut io = state.lock_io();
                        let bytes = bytes::Bytes::copy_from_slice(&buf[..]);
                        drop(buf);
                        io.codec.handle_input(bytes)
                    };
                    if handle_result.is_err() {
                        PullOutcome::ProtoErr
                    } else {
                        state.last_input_nanos.store(
                            state.hb_epoch.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        crate::socket::try_one_shot_large_recv(state, &mut sguard)
                            .await
                            .into()
                    }
                }
            }
        }
    }
}

async fn handle_pull_outcome(
    outcome: PullOutcome,
    state: &Arc<DirectIoState>,
) -> Result<RecvAction> {
    match outcome {
        PullOutcome::Fed => Ok(RecvAction::Proceed),
        PullOutcome::Eof => {
            state.signal_eof();
            Err(Error::Closed)
        }
        PullOutcome::ProtoErr => {
            state.signal_eof();
            Ok(RecvAction::Return(None))
        }
        PullOutcome::Err(e) => {
            let os = e.raw_os_error();
            if os != Some(libc::ENOBUFS) && os != Some(libc::ECANCELED) {
                state.signal_eof();
                return Err(Error::Closed);
            }
            if state.recv_stream.rearm(&state.peer_io).await.is_err() {
                state.signal_eof();
                return Err(Error::Closed);
            }
            state.multishot_rearms.fetch_add(1, Ordering::Relaxed);
            Ok(RecvAction::Proceed)
        }
        PullOutcome::StartAccumulation => {
            let mut io = state.lock_io();
            if let Some((plen, prefix)) = io.codec.begin_supplied_payload_with_prefix() {
                let mut buf = bytes::BytesMut::with_capacity(plen);
                buf.extend_from_slice(prefix.as_slice());
                drop(io);
                *state.pending_acc.lock().expect("pending_acc") = Some(buf);
                state.large_recv_pending.store(plen, Ordering::Release);
            }
            Ok(RecvAction::Proceed)
        }
    }
}

impl Socket {
    fn drain_recv_cache(&self, st: SocketType) -> Result<Option<Message>> {
        if post_recv_needs_type_state(st) {
            loop {
                let raw = self.inner().recv_cache.get().pop_front();
                let Some(raw) = raw else { break };
                if let Some(out) = self.post_recv_apply(raw)? {
                    return Ok(Some(out));
                }
            }
        } else if self.needs_subscription_filter() {
            let cache = self.inner().recv_cache.get();
            while let Some(raw) = cache.pop_front() {
                if self.matches_subscription(&raw) {
                    return Ok(Some(raw));
                }
            }
        } else {
            let cache = self.inner().recv_cache.get();
            if let Some(msg) = cache.pop_front() {
                return Ok(Some(msg));
            }
        }
        Ok(None)
    }

    /// Receive the next message, blocking until one is available or the socket is closed.
    #[expect(clippy::too_many_lines)]
    pub async fn recv(&self) -> Result<Message> {
        use futures::FutureExt;
        let inner = self.inner();
        let st = inner.socket_type;
        if inner.simple_recv {
            let cache = inner.recv_cache.get();
            if let Some(msg) = cache.pop_front() {
                return Ok(msg);
            }
            let dio = unsafe { &*inner.direct_recv_io.get() };
            if let Some(ref state) = *dio
                && let Ok(mut io) = state.peer_io.try_lock()
            {
                while let Some(ev) = io.codec.poll_event() {
                    if let Event::HandshakeSucceeded { .. } = ev {
                        io.handshake_done = true;
                    }
                }
                if !cache.is_empty() {
                    return Ok(cache.pop_front().expect("non-empty"));
                }
                if io.decoder.is_some() {
                    self.drain_decoder_into(&mut io, cache)?;
                } else {
                    io.codec.swap_messages(cache);
                }
                if let Some(msg) = cache.pop_front() {
                    return Ok(msg);
                }
            }
            if let Some(msg) = self.try_direct_recv().await? {
                return Ok(msg);
            }
        } else if direct_recv_eligible(st) {
            if let Some(msg) = self.drain_recv_cache(st)? {
                return Ok(msg);
            }
            if !post_recv_needs_type_state(st) && !self.needs_subscription_filter() {
                let cache = inner.recv_cache.get();
                let dio = unsafe { &*inner.direct_recv_io.get() };
                if let Some(ref state) = *dio
                    && let Ok(mut io) = state.peer_io.try_lock()
                    && let Ok(Some(msg)) = self.drain_and_swap(&mut io, cache)
                {
                    return Ok(msg);
                }
            }
            if let Some(msg) = self.try_direct_recv().await? {
                return Ok(msg);
            }
        }
        loop {
            if let Some(msg) = self.inner().recv_cache.get().pop_front() {
                return Ok(msg);
            }

            let recv_state = unsafe { &mut *self.inner().inproc_recv.get() };
            if !recv_state.consumers.is_empty() {
                let cache = self.inner().recv_cache.get();
                let max = self.inner().options.max_message_size;
                let n = recv_state.consumers.len();
                let start = recv_state.fq_index;
                for i in 0..n {
                    let idx = (start + i) % n;
                    let c = &mut recv_state.consumers[idx];
                    let got = c.prefetch();
                    if got > 0 {
                        while let Some(msg) = c.pop() {
                            if max.is_some_and(|m| msg.byte_len() > m) {
                                continue;
                            }
                            cache.push_back(msg);
                        }
                        c.release();
                    }
                    if !cache.is_empty() {
                        recv_state.fq_index = idx + 1;
                    }
                }
                if let Some(msg) = cache.pop_front() {
                    self.inner().inproc_parked.store(false, Ordering::Release);
                    return Ok(msg);
                }
            }

            match self.inner().in_rx.try_recv() {
                Ok(frame) => {
                    if let Some(msg) = self.process_inbound_frame(frame)? {
                        return Ok(msg);
                    }
                    continue;
                }
                Err(blume::TryRecvError::Disconnected) if recv_state.consumers.is_empty() => {
                    return Err(Error::Closed);
                }
                _ => {}
            }

            let inproc_listener = self.inner().inproc_recv_event.listen();
            let peer_listener = self.inner().on_peer_ready.listen();
            self.inner().inproc_parked.store(true, Ordering::Release);
            let recv_state = unsafe { &mut *self.inner().inproc_recv.get() };
            if recv_state.consumers.iter().any(|c| !c.is_empty()) {
                self.inner().inproc_parked.store(false, Ordering::Release);
                continue;
            }
            let in_rx_fut = self.inner().in_rx.recv_async();
            futures::pin_mut!(inproc_listener);
            futures::pin_mut!(in_rx_fut);
            futures::pin_mut!(peer_listener);
            futures::select_biased! {
                () = inproc_listener.fuse() => {}
                frame = in_rx_fut.fuse() => {
                    self.inner().inproc_parked.store(false, Ordering::Release);
                    let frame = frame.map_err(|_| Error::Closed)?;
                    if let Some(msg) = self.process_inbound_frame(frame)? {
                        return Ok(msg);
                    }
                }
                () = peer_listener.fuse() => {}
            }
            self.inner().inproc_parked.store(false, Ordering::Release);
        }
    }

    fn process_inbound_frame(&self, frame: InboundFrame) -> Result<Option<Message>> {
        let st = self.inner().socket_type;
        match frame {
            InboundFrame::Message(full) => {
                let crate::transport::inproc::InboundMessage { peer_identity, msg } = full;
                if let Some(max) = self.inner().options.max_message_size
                    && msg.byte_len() > max
                {
                    return Ok(None);
                }
                if !self.matches_subscription(&msg) {
                    return Ok(None);
                }
                let msg = if is_identity_recv(st) {
                    let id = peer_identity.unwrap_or_default();
                    Message::with_prefix(id, msg)
                } else {
                    msg
                };
                if post_recv_needs_type_state(st) {
                    self.inner()
                        .type_state
                        .lock()
                        .expect("type_state lock")
                        .post_recv(st, msg)
                } else {
                    Ok(Some(msg))
                }
            }
            InboundFrame::Command(c) => {
                if matches!(st, SocketType::XPub) {
                    use omq_proto::proto::Command;
                    let body = match *c {
                        Command::Subscribe(p) => {
                            let mut buf = bytes::BytesMut::with_capacity(1 + p.len());
                            buf.extend_from_slice(&[0x01]);
                            buf.extend_from_slice(&p);
                            Some(buf.freeze())
                        }
                        Command::Cancel(p) => {
                            let mut buf = bytes::BytesMut::with_capacity(1 + p.len());
                            buf.extend_from_slice(&[0x00]);
                            buf.extend_from_slice(&p);
                            Some(buf.freeze())
                        }
                        _ => None,
                    };
                    if let Some(b) = body {
                        return Ok(Some(Message::single(b)));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Non-blocking receive; returns `Err(WouldBlock)` if no message is queued.
    #[inline]
    pub fn try_recv(&self) -> Result<Message> {
        let inner = self.inner();
        if inner.simple_recv {
            let cache = inner.recv_cache.get();
            if let Some(msg) = cache.pop_front() {
                return Ok(msg);
            }
            let dio = unsafe { &*inner.direct_recv_io.get() };
            if let Some(ref state) = *dio {
                let mut io = state.lock_io();
                while let Some(ev) = io.codec.poll_event() {
                    if let Event::HandshakeSucceeded { .. } = ev {
                        io.handshake_done = true;
                    }
                }
                if !cache.is_empty() {
                    return Ok(cache.pop_front().expect("non-empty"));
                }
                if io.decoder.is_some() {
                    self.drain_decoder_into(&mut io, cache)?;
                } else {
                    io.codec.swap_messages(cache);
                }
                if let Some(msg) = cache.pop_front() {
                    return Ok(msg);
                }
            }
        } else {
            let st = inner.socket_type;
            if direct_recv_eligible(st) {
                if let Some(msg) = self.drain_recv_cache(st)? {
                    return Ok(msg);
                }
                if !post_recv_needs_type_state(st) && !self.needs_subscription_filter() {
                    let dio = unsafe { &*inner.direct_recv_io.get() };
                    if let Some(ref state) = *dio {
                        let cache = inner.recv_cache.get();
                        let mut io = state.lock_io();
                        if let Ok(Some(msg)) = self.drain_and_swap(&mut io, cache) {
                            return Ok(msg);
                        }
                    }
                }
            }
        }
        let recv_state = unsafe { &mut *inner.inproc_recv.get() };
        if !recv_state.consumers.is_empty() {
            let cache = inner.recv_cache.get();
            let max = inner.options.max_message_size;
            for c in &mut recv_state.consumers {
                let got = c.prefetch();
                if got > 0 {
                    while let Some(msg) = c.pop() {
                        if max.is_some_and(|m| msg.byte_len() > m) {
                            continue;
                        }
                        cache.push_back(msg);
                    }
                    c.release();
                }
            }
            if let Some(msg) = cache.pop_front() {
                return Ok(msg);
            }
        }
        loop {
            let frame = inner.in_rx.try_recv().map_err(|e| match e {
                blume::TryRecvError::Empty => Error::WouldBlock,
                blume::TryRecvError::Disconnected => Error::Closed,
            })?;
            if let Some(msg) = self.process_inbound_frame(frame)? {
                return Ok(msg);
            }
        }
    }

    fn snapshot_direct_io_single_peer(&self) -> Option<Arc<DirectIoState>> {
        let peers = self.inner().out_peers.read().expect("peers lock");
        if peers.len() != 1 {
            return None;
        }
        let (_, p) = peers.iter().next()?;
        let handle = p.direct_io.as_ref()?;
        handle.read().expect("direct_io handle lock").clone()
    }

    #[expect(clippy::unused_self)]
    fn drain_and_swap(
        &self,
        io: &mut PeerIo,
        cache: &mut std::collections::VecDeque<Message>,
    ) -> Result<Option<Message>> {
        while let Some(ev) = io.codec.poll_event() {
            match ev {
                Event::Message(_) => unreachable!("messages use poll_message"),
                // Direct-recv sockets (PULL/SUB/PAIR/…) don't process
                // post-handshake commands here; PING/PONG are already
                // handled inside the codec.
                Event::Command(_) => {}
                Event::HandshakeSucceeded { .. } => {
                    io.handshake_done = true;
                }
            }
        }
        if !cache.is_empty() {
            return Ok(cache.pop_front());
        }
        if io.decoder.is_some() {
            while let Some(m) = io.codec.poll_message() {
                let m = if let Some(dec) = io.decoder.as_mut() {
                    match dec.decode(m)? {
                        Some(plain) => plain,
                        None => continue,
                    }
                } else {
                    m
                };
                cache.push_back(m);
            }
        } else {
            io.codec.swap_messages(cache);
        }
        Ok(cache.pop_front())
    }

    #[inline(never)]
    #[expect(clippy::unused_self)]
    fn drain_decoder_into(
        &self,
        io: &mut PeerIo,
        cache: &mut std::collections::VecDeque<Message>,
    ) -> Result<()> {
        while let Some(m) = io.codec.poll_message() {
            let m = if let Some(dec) = io.decoder.as_mut() {
                match dec.decode(m)? {
                    Some(plain) => plain,
                    None => continue,
                }
            } else {
                m
            };
            cache.push_back(m);
        }
        Ok(())
    }

    fn post_recv_apply(&self, msg: Message) -> Result<Option<Message>> {
        if !self.matches_subscription(&msg) {
            return Ok(None);
        }
        let st = self.inner().socket_type;
        if post_recv_needs_type_state(st) {
            Ok(self
                .inner()
                .type_state
                .lock()
                .expect("type_state lock")
                .post_recv(st, msg)?)
        } else {
            Ok(Some(msg))
        }
    }

    fn process_inproc_frame_for_direct(&self, frame: InboundFrame) -> Result<Option<Message>> {
        let max = self.inner().options.max_message_size;
        match frame {
            InboundFrame::Message(full) => {
                let crate::transport::inproc::InboundMessage { msg, .. } = full;
                if max.is_some_and(|m| msg.byte_len() > m) {
                    return Ok(None);
                }
                self.post_recv_apply(msg)
            }
            InboundFrame::Command(_) => Ok(None),
        }
    }

    fn drain_codec_for_recv(&self, state: &Arc<DirectIoState>) -> Result<RecvAction> {
        let drained = {
            let mut io = state.lock_io();
            if !io.handshake_done {
                return Ok(RecvAction::Return(None));
            }
            let cache = self.inner().recv_cache.get();
            self.drain_and_swap(&mut io, cache)?
        };
        let Some(msg) = drained else {
            return Ok(RecvAction::Proceed);
        };
        if let Some(out) = self.post_recv_apply(msg)? {
            return Ok(RecvAction::Return(Some(out)));
        }
        if post_recv_needs_type_state(self.inner().socket_type) {
            loop {
                let raw = self.inner().recv_cache.get().pop_front();
                let Some(raw) = raw else { break };
                if let Some(out) = self.post_recv_apply(raw)? {
                    return Ok(RecvAction::Return(Some(out)));
                }
            }
        } else if self.needs_subscription_filter() {
            let cache = self.inner().recv_cache.get();
            while let Some(raw) = cache.pop_front() {
                if self.matches_subscription(&raw) {
                    return Ok(RecvAction::Return(Some(raw)));
                }
            }
        } else {
            let cache = self.inner().recv_cache.get();
            if let Some(raw) = cache.pop_front() {
                return Ok(RecvAction::Return(Some(raw)));
            }
        }
        Ok(RecvAction::Retry)
    }

    async fn try_direct_recv(&self) -> Result<Option<Message>> {
        use core::pin::pin;
        use futures::FutureExt;
        use futures::future::FusedFuture;

        if !self.inner().in_rx.is_empty() {
            return Ok(None);
        }
        let state = {
            let cur_gen = self.inner().peers_gen.load(Ordering::Acquire);
            let cr = self.inner().cached_route.lock().expect("cached_route");
            if let Some(ref r) = *cr
                && r.generation == cur_gen
            {
                r.direct.clone()
            } else {
                None
            }
        }
        .or_else(|| self.snapshot_direct_io_single_peer());
        let Some(state) = state else {
            return Ok(None);
        };
        // SAFETY: single-threaded compio runtime.
        unsafe { *self.inner().direct_recv_io.get() = Some(state.clone()) };
        if state
            .recv_claim
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Acquire)
            .is_err()
        {
            return Ok(None);
        }
        let guard = ClaimGuard { state: &state };
        if !self.inner().in_rx.is_empty() {
            return Ok(None);
        }

        let mut inrx_fut = pin!(self.inner().in_rx.recv_async().fuse());
        let mut codec_ready_fut = pin!(state.recv_codec_ready.listen().fuse());

        loop {
            match accumulate_large_recv(&state).await? {
                RecvAction::Return(msg) => return Ok(msg),
                RecvAction::Retry | RecvAction::Proceed => {}
            }

            match self.drain_codec_for_recv(&state)? {
                RecvAction::Return(msg) => return Ok(msg),
                RecvAction::Retry => continue,
                RecvAction::Proceed => {}
            }

            if codec_ready_fut.is_terminated() {
                codec_ready_fut.set(state.recv_codec_ready.listen().fuse());
            }

            let pf = pull_and_feed(&state);
            futures::pin_mut!(pf);
            let outcome = futures::select_biased! {
                frame = inrx_fut.as_mut() => {
                    let frame = frame.map_err(|_| Error::Closed)?;
                    drop(guard);
                    return self.process_inproc_frame_for_direct(frame);
                }
                () = codec_ready_fut.as_mut() => PullOutcome::Fed,
                outcome = pf.fuse() => outcome,
            };

            match outcome {
                PullOutcome::Fed if !self.inner().in_rx.is_empty() => {
                    drop(guard);
                    return Ok(None);
                }
                outcome => match handle_pull_outcome(outcome, &state).await? {
                    RecvAction::Return(msg) => return Ok(msg),
                    RecvAction::Retry | RecvAction::Proceed => {}
                },
            }
            Self::flush_codec_output(&state);
        }
    }

    fn flush_codec_output(state: &Arc<DirectIoState>) {
        let mut io = state.lock_io();
        if !io.codec.has_pending_transmit() {
            return;
        }
        let chunks = io.codec.clone_transmit_chunks();
        let total: usize = chunks.iter().map(Bytes::len).sum();
        io.codec.advance_transmit(total);
        drop(io);
        if !chunks.is_empty() {
            state.encoded_queue.borrow_mut().push_raw(chunks);
            if state.driver_in_select.get() {
                state.transmit_ready.notify(1);
            }
        }
    }

    #[inline]
    fn needs_subscription_filter(&self) -> bool {
        matches!(
            self.inner().socket_type,
            SocketType::Sub | SocketType::XSub | SocketType::Dish
        )
    }

    fn matches_subscription(&self, msg: &Message) -> bool {
        if !matches!(
            self.inner().socket_type,
            SocketType::Sub | SocketType::XSub | SocketType::Dish
        ) {
            return true;
        }
        match self.inner().socket_type {
            SocketType::Sub | SocketType::XSub => {
                let topic = msg.part_bytes(0).unwrap_or_default();
                self.inner()
                    .subscriptions
                    .read()
                    .expect("subscriptions lock")
                    .matches(&topic)
            }
            SocketType::Dish => {
                let group = msg.part_bytes(0).unwrap_or_default();
                self.inner()
                    .joined_groups
                    .read()
                    .expect("joined_groups lock")
                    .contains(&group[..])
            }
            _ => true,
        }
    }
}
