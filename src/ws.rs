use crate::AwarenessRef;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use lib0::decoding::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::spawn;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use warp::ws::WebSocket;
use y_sync::awareness::Awareness;
use y_sync::sync::{DefaultProtocol, Error, Message, MessageReader, Protocol, SyncMessage};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::Update;

/// Connection Wrapper over a [WebSocket], which implements a Yjs/Yrs awareness and update exchange
/// protocol.
///
/// This connection implements Future pattern and can be awaited upon in order for a caller to
/// recognize whether underlying websocket connection has been finished gracefully or abruptly.
///
/// Examples:
/// ```rust
/// use std::sync::Arc;
/// use tokio::sync::RwLock;
/// use warp::{Filter,Reply,Rejection};
/// use warp::ws::{WebSocket,Ws};
/// use yrs::Doc;
/// use y_sync::awareness::Awareness;
/// use yrs_warp::ws::WarpConn;
///
/// async fn handle(ws: Ws, awareness: Arc<RwLock<Awareness>>) -> Result<impl Reply, Rejection> {
///     Ok(ws.on_upgrade(move |socket| async {
///         let conn = WarpConn::new(awareness, socket);
///         if let Err(e) = conn.await {
///             eprintln!("connection finished abruptly because of '{}'", e);
///         }
///     }))
/// }
///
/// fn server() {
///    // We're using a single static document shared among all the peers.
///    let awareness = Arc::new(RwLock::new(Awareness::new(Doc::new())));
///
///    let ws = warp::path("my-room")
///         .and(warp::ws())
///         .and_then(move |ws: Ws| handle(ws, awareness.clone()));
///     // warp::serve(ws).run(([0, 0, 0, 0], 8000)).await;
/// }
/// ```
pub struct WarpConn {
    processing_loop: JoinHandle<Result<(), Error>>,
    inbox: ConnInbox,
}

impl WarpConn {
    /// Wraps incoming [WebSocket] connection and supplied [Awareness] accessor into a new
    /// connection handler capable of exchanging Yrs/Yjs messages.
    ///
    /// While creation of new [WarpConn] always succeeds, a connection itself can possibly fail
    /// while processing incoming input/output. This can be detected by awaiting for returned
    /// [WarpConn] and handling the awaited result.
    pub fn new(awareness_ref: AwarenessRef, ws: WebSocket) -> Self {
        Self::with_protocol(awareness_ref, ws, DefaultProtocol)
    }

    /// Wraps incoming [WebSocket] connection and supplied [Awareness] accessor into a new
    /// connection handler capable of exchanging Yrs/Yjs messages.
    ///
    /// While creation of new [WarpConn] always succeeds, a connection itself can possibly fail
    /// while processing incoming input/output. This can be detected by awaiting for returned
    /// [WarpConn] and handling the awaited result.
    pub fn with_protocol<P>(awareness_ref: AwarenessRef, ws: WebSocket, protocol: P) -> Self
    where
        P: Protocol + Send + Sync + 'static,
    {
        let (sink, mut source) = ws.split();
        let mut sink = Arc::new(Mutex::new(sink));
        let inbox = ConnInbox(sink.clone());
        let processing_loop: JoinHandle<Result<(), Error>> = spawn(async move {
            // at the beginning send SyncStep1 and AwarenessUpdate
            let payload = {
                let mut encoder = EncoderV1::new();
                let awareness = awareness_ref.read().await;
                protocol.start(&awareness, &mut encoder)?;
                encoder.to_vec()
            };
            if !payload.is_empty() {
                let mut s = sink.lock().await;
                if let Err(e) = s.send(warp::ws::Message::binary(payload)).await {
                    return Err(e.to_err());
                }
            }

            while let Some(input) = source.next().await {
                match Self::process(&protocol, &awareness_ref, &mut sink, input).await {
                    Ok(()) => { /* continue */ }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            Ok(())
        });
        WarpConn {
            processing_loop,
            inbox,
        }
    }

    /// Returns a reference to a [ConnInbox] that can be used to send data through an underlying
    /// websocket connection.
    pub fn inbox(&self) -> &ConnInbox {
        &self.inbox
    }

    async fn process<P: Protocol>(
        protocol: &P,
        awareness: &AwarenessRef,
        sink: &mut Arc<Mutex<SplitSink<WebSocket, warp::ws::Message>>>,
        input: Result<warp::ws::Message, warp::Error>,
    ) -> Result<(), Error> {
        match input {
            Ok(input) => {
                let mut decoder = DecoderV1::new(Cursor::new(input.as_bytes()));
                let reader = MessageReader::new(&mut decoder);
                for r in reader {
                    let msg = r?;
                    if let Some(reply) = handle_msg(protocol, &awareness, msg).await? {
                        let mut sender = sink.lock().await;
                        if let Err(e) = sender
                            .send(warp::ws::Message::binary(reply.encode_v1()))
                            .await
                        {
                            return Err(Error::Other(e.into()));
                        }
                    }
                }
                Ok(())
            }
            Err(e) => Err(Error::Other(e.into())),
        }
    }
}

impl core::future::Future for WarpConn {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.processing_loop).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Other(e.into()))),
            Poll::Ready(Ok(r)) => Poll::Ready(r),
        }
    }
}

/// An asynchronous trait that defines a capability to send custom messages to an implementing type
/// in an asynchronous and potentially fail-prone fashion.
#[async_trait::async_trait]
pub trait Inbox {
    type Item;
    type Error: std::error::Error + Send + Sync;

    async fn send(&mut self, item: Self::Item) -> Result<(), Self::Error>;
}

/// An input entry for a related [WarpConn]. It can be used to send the binary data to a correlated
/// websocket connection. This structure implements a [Clone] trait and can be safely passed to a
/// different tasks.
#[derive(Debug, Clone)]
pub struct ConnInbox(Arc<Mutex<SplitSink<WebSocket, warp::ws::Message>>>);

#[async_trait::async_trait]
impl Inbox for ConnInbox {
    type Item = Vec<u8>;
    type Error = Error;

    async fn send(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        let mut g = self.0.lock().await;
        g.send(warp::ws::Message::binary(item))
            .await
            .map_err(|e| e.to_err())?;
        Ok(())
    }
}

trait ToError {
    fn to_err(self) -> Error;
}

impl ToError for warp::Error {
    fn to_err(self) -> Error {
        Error::Other(self.into())
    }
}

async fn handle_msg<P: Protocol>(
    protocol: &P,
    a: &Arc<RwLock<Awareness>>,
    msg: Message,
) -> Result<Option<Message>, Error> {
    match msg {
        Message::Sync(msg) => match msg {
            SyncMessage::SyncStep1(sv) => {
                let awareness = a.read().await;
                protocol.handle_sync_step1(&awareness, sv)
            }
            SyncMessage::SyncStep2(update) => {
                let mut awareness = a.write().await;
                protocol.handle_sync_step2(&mut awareness, Update::decode_v1(&update)?)
            }
            SyncMessage::Update(update) => {
                let mut awareness = a.write().await;
                protocol.handle_update(&mut awareness, Update::decode_v1(&update)?)
            }
        },
        Message::Auth(reason) => {
            let awareness = a.read().await;
            protocol.handle_auth(&awareness, reason)
        }
        Message::AwarenessQuery => {
            let awareness = a.read().await;
            protocol.handle_awareness_query(&awareness)
        }
        Message::Awareness(update) => {
            let mut awareness = a.write().await;
            protocol.handle_awareness_update(&mut awareness, update)
        }
        Message::Custom(tag, data) => {
            let mut awareness = a.write().await;
            protocol.missing_handle(&mut awareness, tag, data)
        }
    }
}
