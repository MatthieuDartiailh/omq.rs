//! WebSocket bind/connect glue (ZWS/2.0, RFC 45).

use std::net::SocketAddr;

use compio::net::{TcpListener, TcpStream};
use compio_ws::tungstenite::client::IntoClientRequest;
use compio_ws::tungstenite::http::{HeaderValue, Request, Response, StatusCode};

use omq_proto::endpoint::{Endpoint, Host};
use omq_proto::error::{Error, Result};

pub(crate) type WsStream = compio_ws::WebSocketStream<TcpStream>;

const SUBPROTOCOL: &str = "ZWS2.0";
const SUBPROTOCOL_NULL: &str = "ZWS2.0/NULL";

fn resolve_bind(host: &Host, port: u16) -> Result<SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    match host {
        Host::Wildcard => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(_) => Err(Error::InvalidEndpoint(
            "DNS resolution not yet supported on omq-compio WS".into(),
        )),
    }
}

fn resolve_connect(host: &Host, port: u16) -> Result<SocketAddr> {
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(_) => Err(Error::InvalidEndpoint(
            "DNS resolution not yet supported on omq-compio WS".into(),
        )),
    }
}

fn ws_config() -> compio_ws::Config {
    compio_ws::Config::new().disable_nagle(true)
}

pub(crate) struct WsListener {
    pub(crate) inner: TcpListener,
    pub(crate) local_addr: SocketAddr,
}

pub(crate) async fn bind(endpoint: &Endpoint) -> Result<WsListener> {
    let (host, port) = match endpoint {
        Endpoint::Ws { host, port, .. } | Endpoint::Wss { host, port, .. } => (host, *port),
        _ => {
            return Err(Error::InvalidEndpoint(format!(
                "ws transport got non-ws endpoint: {endpoint}"
            )));
        }
    };
    let addr = resolve_bind(host, port)?;
    let listener = TcpListener::bind(addr).await.map_err(Error::Io)?;
    let local = listener.local_addr().map_err(Error::Io)?;
    Ok(WsListener {
        inner: listener,
        local_addr: local,
    })
}

fn ws_io_err(e: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::other(e.to_string()))
}

#[allow(clippy::result_large_err)]
pub(crate) async fn accept(stream: TcpStream) -> Result<WsStream> {
    let _ = stream.set_nodelay(true);
    let ws = compio_ws::accept_hdr_with_config_async(
        stream,
        |req: &Request<()>,
         mut resp: Response<()>|
         -> std::result::Result<Response<()>, Response<Option<String>>> {
            let protocols = req
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let chosen = protocols
                .split(',')
                .map(str::trim)
                .find(|p| *p == SUBPROTOCOL_NULL || *p == SUBPROTOCOL);
            if let Some(proto) = chosen {
                resp.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    HeaderValue::from_str(proto).expect("valid header"),
                );
                Ok(resp)
            } else {
                let mut err_resp: Response<Option<String>> =
                    Response::new(Some("no supported ZWS subprotocol".into()));
                *err_resp.status_mut() = StatusCode::BAD_REQUEST;
                Err(err_resp)
            }
        },
        ws_config(),
    )
    .await
    .map_err(ws_io_err)?;
    Ok(ws)
}

pub(crate) async fn connect(endpoint: &Endpoint) -> Result<WsStream> {
    let (host, port, path) = match endpoint {
        Endpoint::Ws {
            host, port, path, ..
        }
        | Endpoint::Wss {
            host, port, path, ..
        } => (host, *port, path.as_str()),
        _ => {
            return Err(Error::InvalidEndpoint(format!(
                "ws transport got non-ws endpoint: {endpoint}"
            )));
        }
    };
    let addr = resolve_connect(host, port)?;
    let stream = TcpStream::connect(addr).await.map_err(Error::Io)?;
    let _ = stream.set_nodelay(true);

    let url = format!("ws://{host}:{port}{path}");
    let mut request = url.into_client_request().map_err(ws_io_err)?;
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static(SUBPROTOCOL_NULL),
    );

    let (ws, _response) = compio_ws::client_async_with_config(request, stream, ws_config())
        .await
        .map_err(ws_io_err)?;
    Ok(ws)
}
