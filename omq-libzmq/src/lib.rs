//! omq-libzmq -- libzmq-compatible C interface backed by omq-tokio.
#![expect(clippy::not_unsafe_ptr_arg_deref)]

mod consts;
mod context;
pub mod curve;
mod error;
mod inproc_bypass;
mod local_cell;
mod msg;
mod notify;
mod opts;
pub mod poll;
pub mod proxy;
mod send_recv;
mod socket;
mod util;

pub use context::{
    zmq_ctx_destroy, zmq_ctx_get, zmq_ctx_new, zmq_ctx_set, zmq_ctx_shutdown, zmq_ctx_term,
    zmq_init, zmq_term,
};
pub use curve::{zmq_curve_keypair, zmq_curve_public, zmq_z85_decode, zmq_z85_encode};
pub use error::{zmq_errno, zmq_strerror};
pub use msg::{
    zmq_msg_close, zmq_msg_copy, zmq_msg_data, zmq_msg_get, zmq_msg_gets, zmq_msg_group,
    zmq_msg_init, zmq_msg_init_buffer, zmq_msg_init_data, zmq_msg_init_size, zmq_msg_more,
    zmq_msg_move, zmq_msg_recv, zmq_msg_routing_id, zmq_msg_send, zmq_msg_set, zmq_msg_set_group,
    zmq_msg_set_routing_id, zmq_msg_size, zmq_recvmsg, zmq_sendmsg,
};
pub use poll::zmq_poll;
pub use proxy::{zmq_proxy, zmq_proxy_steerable};
pub use send_recv::{zmq_recv, zmq_send, zmq_send_const};
pub use socket::{
    zmq_bind, zmq_close, zmq_connect, zmq_disconnect, zmq_join, zmq_leave, zmq_socket,
    zmq_socket_monitor, zmq_unbind,
};
pub use util::{
    zmq_atomic_counter_dec, zmq_atomic_counter_destroy, zmq_atomic_counter_inc,
    zmq_atomic_counter_new, zmq_atomic_counter_set, zmq_atomic_counter_value, zmq_has, zmq_sleep,
    zmq_stopwatch_intermediate, zmq_stopwatch_start, zmq_stopwatch_stop, zmq_version,
};

// The opts module exports setsockopt/getsockopt directly as C symbols.
pub use opts::{zmq_getsockopt, zmq_setsockopt};

const _: () = assert!(std::mem::size_of::<msg::OmqMsgRepr>() == 64);
