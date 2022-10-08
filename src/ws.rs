use crate::awareness::{Awareness, AwarenessRef, AwarenessUpdate, Event};
use crate::sync::MSG_SYNC_UPDATE;
use crate::{awareness, sync};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use lib0::decoding::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::spawn;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio::task::{JoinError, JoinHandle};
use warp::ws::WebSocket;
use yrs::updates::decoder::{Decode, Decoder, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder};
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
/// use yrs_warp::awareness::Awareness;
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
    pub fn new(awareness: AwarenessRef, ws: WebSocket) -> Self {
        let (sink, mut source) = ws.split();
        let mut sink = Arc::new(Mutex::new(sink));
        let inbox = ConnInbox(sink.clone());
        let processing_loop = spawn(async move {
            // at the beginning send SyncStep1 and AwarenessUpdate
            if let Err(e) = Self::init(&awareness, &mut sink).await {
                return Err(e);
            }

            while let Some(input) = source.next().await {
                match Self::process(&awareness, &mut sink, input).await {
                    Ok(()) => { /* continue */ }
                    ret => {
                        return ret;
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

    pub fn inbox(&self) -> &ConnInbox {
        &self.inbox
    }

    async fn init(
        awareness: &AwarenessRef,
        sink: &mut Arc<Mutex<SplitSink<WebSocket, warp::ws::Message>>>,
    ) -> Result<(), Error> {
        let (sv, update) = {
            let awareness = awareness.read().await;
            let sv = awareness.doc().transact().state_vector();
            let update = awareness.update()?;
            (sv, update)
        };
        let sync_step_1 = Message::Sync(sync::Message::SyncStep1(sv)).encode_v1();
        let update = Message::Awareness(update).encode_v1();

        let mut sender = sink.lock().await;
        sender.send(warp::ws::Message::binary(sync_step_1)).await?;
        sender.send(warp::ws::Message::binary(update)).await?;
        Ok(())
    }

    async fn process(
        awareness: &AwarenessRef,
        sink: &mut Arc<Mutex<SplitSink<WebSocket, warp::ws::Message>>>,
        input: Result<warp::ws::Message, warp::Error>,
    ) -> Result<(), Error> {
        let input = input?;
        let mut decoder = DecoderV1::new(Cursor::new(input.as_bytes()));
        while {
            // it's possible that input WS message aggregates more than one y-protocol message
            match Message::decode(&mut decoder) {
                Ok(msg) => {
                    if let Some(reply) = handle_msg(&awareness, msg).await? {
                        let mut sender = sink.lock().await;
                        sender
                            .send(warp::ws::Message::binary(reply.encode_v1()))
                            .await?;
                    }
                    true
                }
                Err(lib0::error::Error::EndOfBuffer) => false,
                Err(error) => return Err(Error::DecodingError(error)),
            }
        } {}
        Ok(())
    }
}

impl core::future::Future for WarpConn {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.processing_loop).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Schedule(e))),
            Poll::Ready(Ok(r)) => Poll::Ready(r),
        }
    }
}

#[async_trait::async_trait]
pub trait Inbox {
    type Item;
    type Error: std::error::Error + Send + Sync;

    async fn send(&mut self, item: Self::Item) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone)]
pub struct ConnInbox(Arc<Mutex<SplitSink<WebSocket, warp::ws::Message>>>);

#[async_trait::async_trait]
impl Inbox for ConnInbox {
    type Item = warp::ws::Message;
    type Error = Error;

    async fn send(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        let mut g = self.0.lock().await;
        g.send(item).await?;
        Ok(())
    }
}

pub struct BroadcastGroup {
    awareness_ref: AwarenessRef,
    sender: Sender<warp::ws::Message>,
    receiver: Receiver<warp::ws::Message>,
    awareness_sub: awareness::Subscription<Event>,
    doc_sub: yrs::Subscription<yrs::UpdateEvent>,
}

unsafe impl Send for BroadcastGroup {}
unsafe impl Sync for BroadcastGroup {}

impl BroadcastGroup {
    pub async fn open(awareness_ref: AwarenessRef, capacity: usize) -> Self {
        let (sender, receiver) = channel(capacity);
        let (doc_sub, awareness_sub) = {
            let mut awareness = awareness_ref.write().await;
            let sink = sender.clone();
            let doc_sub = awareness.doc_mut().observe_update_v1(move |_txn, u| {
                println!("broadcasting document update: {:?}", u.update);
                let mut bin = Vec::with_capacity(u.update.len() + 2);
                bin.push(MSG_SYNC);
                bin.push(MSG_SYNC_UPDATE);
                bin.extend_from_slice(&u.update);
                let msg = warp::ws::Message::binary(bin);
                if let Err(e) = sink.send(msg) {
                    eprintln!("couldn't broadcast the document update: {}", e);
                }
            });
            let sink = sender.clone();
            let awareness_sub = awareness.on_update(move |awareness, e| {
                let added = e.added();
                let updated = e.updated();
                let removed = e.removed();
                let mut changed = Vec::with_capacity(added.len() + updated.len() + removed.len());
                changed.extend_from_slice(added);
                changed.extend_from_slice(updated);
                changed.extend_from_slice(removed);

                if let Ok(u) = awareness.update_with_clients(changed) {
                    println!("broadcasting awareness update: {:?}", u);
                    let bin = Message::Awareness(u).encode_v1();
                    let msg = warp::ws::Message::binary(bin);
                    if let Err(e) = sink.send(msg) {
                        eprintln!("couldn't broadcast awareness update: {}", e)
                    }
                }
            });
            (doc_sub, awareness_sub)
        };
        BroadcastGroup {
            awareness_ref,
            sender,
            receiver,
            awareness_sub,
            doc_sub,
        }
    }

    pub fn join<I>(&self, mut inbox: I) -> JoinHandle<Result<(), I::Error>>
    where
        I: Inbox<Item = warp::ws::Message> + Send + Sync + 'static,
    {
        let mut receiver = self.sender.subscribe();
        println!("new inbox joined to broadcasting group");
        spawn(async move {
            while let Ok(msg) = receiver.recv().await {
                println!("broadcasted message to inbox");
                if let Err(e) = inbox.send(msg).await {
                    return Err(e);
                }
            }
            Ok(())
        })
    }
}

const MSG_SYNC: u8 = 0;
const MSG_AWARENESS: u8 = 1;
const MSG_AUTH: u8 = 2;
const MSG_QUERY_AWARENESS: u8 = 3;

const PERMISSION_DENIED: u8 = 0;

async fn handle_msg(a: &Arc<RwLock<Awareness>>, msg: Message) -> Result<Option<Message>, Error> {
    match msg {
        Message::Sync(msg) => match msg {
            sync::Message::SyncStep1(sv) => {
                let awareness = a.read().await;
                let update = awareness.doc().encode_state_as_update_v1(&sv);
                Ok(Some(Message::Sync(sync::Message::SyncStep2(update))))
            }
            sync::Message::SyncStep2(update) | sync::Message::Update(update) => {
                let awareness = a.write().await;
                let mut txn = awareness.doc().transact();
                let update = Update::decode_v1(&update)?;
                txn.apply_update(update);
                Ok(None)
            }
        },
        Message::Auth(reason) => {
            if let Some(reason) = reason {
                Err(Error::PermissionDenied { reason })
            } else {
                Ok(None)
            }
        }
        Message::AwarenessQuery => {
            let awareness = a.read().await;
            let update = awareness.update()?;
            Ok(Some(Message::Awareness(update)))
        }
        Message::Awareness(update) => {
            let mut awareness = a.write().await;
            awareness.apply_update(update)?;
            Ok(None)
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Message {
    Sync(sync::Message),
    Auth(Option<String>),
    AwarenessQuery,
    Awareness(AwarenessUpdate),
}

impl Encode for Message {
    fn encode<E: Encoder>(&self, encoder: &mut E) {
        match self {
            Message::Sync(msg) => {
                encoder.write_var(MSG_SYNC);
                msg.encode(encoder);
            }
            Message::Auth(reason) => {
                encoder.write_var(MSG_AUTH);
                if let Some(reason) = reason {
                    encoder.write_var(PERMISSION_DENIED);
                    encoder.write_string(&reason);
                } else {
                    encoder.write_var(1);
                }
            }
            Message::AwarenessQuery => {
                encoder.write_var(MSG_QUERY_AWARENESS);
            }
            Message::Awareness(update) => {
                encoder.write_var(MSG_AWARENESS);
                encoder.write_buf(&update.encode_v1())
            }
        }
    }
}

impl Decode for Message {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, lib0::error::Error> {
        let tag: u8 = decoder.read_var()?;
        match tag {
            MSG_SYNC => {
                let msg = sync::Message::decode(decoder)?;
                Ok(Message::Sync(msg))
            }
            MSG_AWARENESS => {
                let data = decoder.read_buf()?;
                let update = AwarenessUpdate::decode_v1(data)?;
                Ok(Message::Awareness(update))
            }
            MSG_AUTH => {
                let reason = if decoder.read_var::<u8>()? == PERMISSION_DENIED {
                    Some(decoder.read_string()?.to_string())
                } else {
                    None
                };
                Ok(Message::Auth(reason))
            }
            MSG_QUERY_AWARENESS => Ok(Message::AwarenessQuery),
            _ => Err(lib0::error::Error::UnexpectedValue),
        }
    }
}

/// An error type returned in responde for awaiting for [WarpConn] to complete.
#[derive(Debug, Error)]
pub enum Error {
    /// Incoming Y-protocol message couldn't be deserialized.
    #[error("failed to deserialize message: {0}")]
    DecodingError(#[from] lib0::error::Error),

    /// Applying incoming Y-protocol awareness update has failed.
    #[error("failed to process awareness update: {0}")]
    AwarenessEncoding(#[from] awareness::Error),

    /// An incoming Y-protocol authorization request has been denied.
    #[error("permission denied to access: {reason}")]
    PermissionDenied { reason: String },

    /// Awaiting for scheduled a [WarpConn] execution caused tokio runtime to fail.
    #[error("tokio runtime join handle error occurred, {0}")]
    Schedule(#[from] JoinError),

    /// Custom dynamic kind of error, usually related to a warp internal error messages.
    #[error("internal failure: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<warp::Error> for Error {
    fn from(e: warp::Error) -> Self {
        Error::Other(e.into())
    }
}

#[cfg(test)]
mod test {
    use crate::awareness::Awareness;
    use crate::sync;
    use crate::ws::Message;
    use yrs::updates::decoder::Decode;
    use yrs::updates::encoder::Encode;
    use yrs::{Doc, StateVector};

    #[test]
    fn message_encoding() {
        let doc = Doc::new();
        let txt = doc.transact().get_text("text");
        txt.push(&mut doc.transact(), "hello world");
        let mut awareness = Awareness::new(doc);
        awareness.set_local_state("{\"user\":{\"name\":\"Anonymous 50\",\"color\":\"#30bced\",\"colorLight\":\"#30bced33\"}}");

        let messages = [
            Message::Sync(sync::Message::SyncStep1(
                awareness.doc().transact().state_vector(),
            )),
            Message::Sync(sync::Message::SyncStep2(
                awareness
                    .doc()
                    .encode_state_as_update_v1(&StateVector::default()),
            )),
            Message::Awareness(awareness.update().unwrap()),
            Message::Auth(Some("reason".to_string())),
            Message::AwarenessQuery,
        ];

        for msg in messages {
            let encoded = msg.encode_v1();
            let decoded =
                Message::decode_v1(&encoded).expect(&format!("failed to decode {:?}", msg));
            assert_eq!(decoded, msg);
        }
    }
}
