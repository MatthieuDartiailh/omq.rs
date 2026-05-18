use std::sync::{Arc, atomic::Ordering};

use bytes::Bytes;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::{Event, SocketType};

use crate::transport::inproc::InprocFrame;
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
    matches!(
        t,
        SocketType::Router | SocketType::Server | SocketType::Peer | SocketType::Rep
    )
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
            let fd = {
                let io = state.peer_io.lock().expect("peer_io");
                io.reader.fd_clone()
            };
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
            let mut io = state.peer_io.lock().expect("peer_io");
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
                    let mut io = state.peer_io.lock().expect("peer_io");
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
            Some(Err(e)) if e.raw_os_error() == Some(105) => {
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
                None => PullOutcome::Eof,
                Some(Err(e)) => PullOutcome::Err(e),
                Some(Ok(buf)) => {
                    if buf.is_empty() {
                        return PullOutcome::Eof;
                    }
                    let handle_result = {
                        let mut io = state.peer_io.lock().expect("peer_io");
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
            if e.raw_os_error() != Some(105) {
                state.signal_eof();
                return Err(Error::Closed);
            }
            if state.recv_stream.rearm(&state.peer_io).await.is_err() {
                state.signal_eof();
                return Err(Error::Closed);
            }
            Ok(RecvAction::Proceed)
        }
        PullOutcome::StartAccumulation => {
            let mut io = state.peer_io.lock().expect("peer_io");
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

    pub async fn recv(&self) -> Result<Message> {
        use futures::FutureExt;
        let st = self.inner().socket_type;
        if direct_recv_eligible(st) {
            if let Some(msg) = self.drain_recv_cache(st)? {
                return Ok(msg);
            }
            if !post_recv_needs_type_state(st) && !self.needs_subscription_filter() {
                let cache = self.inner().recv_cache.get();
                let dio = unsafe { &*self.inner().direct_recv_io.get() };
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
                let n = recv_state.consumers.len();
                let start = recv_state.fq_index;
                for i in 0..n {
                    let idx = (start + i) % n;
                    if let Some(msg) = recv_state.consumers[idx].prefetch_and_pop() {
                        recv_state.fq_index = idx + 1;
                        if self
                            .inner()
                            .options
                            .max_message_size
                            .is_some_and(|max| msg.byte_len() > max)
                        {
                            continue;
                        }
                        self.inner().inproc_parked.store(false, Ordering::Release);
                        return Ok(msg);
                    }
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

    fn process_inbound_frame(&self, frame: InprocFrame) -> Result<Option<Message>> {
        let st = self.inner().socket_type;
        match frame {
            InprocFrame::Message(boxed) => {
                let crate::transport::inproc::InprocFullMessage { peer_identity, msg } = *boxed;
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
            InprocFrame::Command(c) => {
                if matches!(st, SocketType::XPub) {
                    use omq_proto::proto::Command;
                    let body = match c {
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

    #[inline]
    pub fn try_recv(&self) -> Result<Message> {
        let st = self.inner().socket_type;
        if direct_recv_eligible(st) {
            if let Some(msg) = self.drain_recv_cache(st)? {
                return Ok(msg);
            }
            if !post_recv_needs_type_state(st) && !self.needs_subscription_filter() {
                let dio = unsafe { &*self.inner().direct_recv_io.get() };
                if let Some(ref state) = *dio {
                    let cache = self.inner().recv_cache.get();
                    let mut io = state.peer_io.lock().expect("peer_io");
                    if let Ok(Some(msg)) = self.drain_and_swap(&mut io, cache) {
                        return Ok(msg);
                    }
                }
            }
        }
        let recv_state = unsafe { &mut *self.inner().inproc_recv.get() };
        let max = self.inner().options.max_message_size;
        for c in &mut recv_state.consumers {
            if let Some(msg) = c.prefetch_and_pop() {
                if max.is_some_and(|m| msg.byte_len() > m) {
                    continue;
                }
                return Ok(msg);
            }
        }
        loop {
            let frame = self.inner().in_rx.try_recv().map_err(|e| match e {
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
        let p = &peers[0];
        let handle = p.direct_io.as_ref()?;
        handle.read().expect("direct_io handle lock").clone()
    }

    #[allow(clippy::unused_self)]
    fn drain_and_swap(
        &self,
        io: &mut PeerIo,
        cache: &mut std::collections::VecDeque<Message>,
    ) -> Result<Option<Message>> {
        while let Some(ev) = io.codec.poll_event() {
            match ev {
                Event::Message(_) => unreachable!("messages use poll_message"),
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

    fn process_inproc_frame_for_direct(&self, frame: InprocFrame) -> Result<Option<Message>> {
        let max = self.inner().options.max_message_size;
        match frame {
            InprocFrame::Message(boxed) => {
                let crate::transport::inproc::InprocFullMessage { msg, .. } = *boxed;
                if max.is_some_and(|m| msg.byte_len() > m) {
                    return Ok(None);
                }
                self.post_recv_apply(msg)
            }
            InprocFrame::Command(_) => Ok(None),
        }
    }

    fn drain_codec_for_recv(&self, state: &Arc<DirectIoState>) -> Result<RecvAction> {
        let drained = {
            let mut io = state.peer_io.lock().expect("peer_io");
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
        use futures::FutureExt;

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

            let pf = pull_and_feed(&state);
            let inrx_fut = self.inner().in_rx.recv_async();
            let codec_ready_fut = state.recv_codec_ready.listen();
            futures::pin_mut!(pf);
            futures::pin_mut!(inrx_fut);
            futures::pin_mut!(codec_ready_fut);
            let outcome = futures::select_biased! {
                frame = inrx_fut.fuse() => {
                    let frame = frame.map_err(|_| Error::Closed)?;
                    drop(guard);
                    return self.process_inproc_frame_for_direct(frame);
                }
                () = codec_ready_fut.fuse() => PullOutcome::Fed,
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
            Self::flush_codec_output(&state).await?;
        }
    }

    async fn flush_codec_output(state: &Arc<DirectIoState>) -> Result<()> {
        loop {
            let mut writer = state.writer.lock().await;
            let chunks = {
                let io = state.peer_io.lock().expect("peer_io");
                if !io.codec.has_pending_transmit() {
                    break;
                }
                let mut c = io.codec.clone_transmit_chunks();
                if c.len() > 1024 {
                    c.truncate(1024);
                }
                c
            };
            if chunks.is_empty() {
                break;
            }
            let (res, _returned) = writer.write_vectored(chunks).await;
            let written = res.map_err(Error::Io)?;
            if written == 0 {
                state.signal_eof();
                return Err(Error::Closed);
            }
            state
                .peer_io
                .lock()
                .expect("peer_io")
                .codec
                .advance_transmit(written);
        }
        Ok(())
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
