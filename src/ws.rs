use crate::awareness::{Awareness, AwarenessUpdate};
use crate::{awareness, sync};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::task::{JoinError, JoinHandle};
use warp::ws::WebSocket;
use yrs::updates::decoder::{Decode, Decoder};
use yrs::updates::encoder::{Encode, Encoder};
use yrs::Update;

pub struct WarpConn {
    processing_loop: JoinHandle<Result<(), Error>>,
}

impl WarpConn {
    pub fn new(awareness: Arc<RwLock<Awareness>>, ws: WebSocket) -> Self {
        let (mut sink, mut source) = ws.split();
        let processing_loop = spawn(async move {
            while let Some(input) = source.next().await {
                match Self::process(&awareness, &mut sink, input).await {
                    Ok(()) => { /* continue */ }
                    error => {
                        return error;
                    }
                }
            }
            Ok(())
        });
        WarpConn { processing_loop }
    }

    async fn process(
        awareness: &Arc<RwLock<Awareness>>,
        sink: &mut SplitSink<WebSocket, warp::ws::Message>,
        input: Result<warp::ws::Message, warp::Error>,
    ) -> Result<(), Error> {
        let msg = Message::decode_v1(input?.as_bytes())?;
        if let Some(reply) = handle_msg(&awareness, msg).await? {
            sink.send(warp::ws::Message::binary(reply.encode_v1()))
                .await?;
        }
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
            sync::Message::SyncStep2(update) => {
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

#[derive(Debug)]
pub enum Message {
    Sync(sync::Message),
    Auth(Option<String>),
    AwarenessQuery,
    Awareness(AwarenessUpdate),
}

impl Encode for Message {
    fn encode<E: Encoder>(&self, encoder: &mut E) {
        encoder.write_var(MSG_SYNC);
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
                encoder.write_all(&update.encode_v1())
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

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to deserialize message")]
    DecodingError(#[from] lib0::error::Error),
    #[error("failed to process awareness update")]
    AwarenessEncoding(#[from] awareness::Error),
    #[error("permission denied to access.\n{reason:?}")]
    PermissionDenied { reason: String },
    #[error("tokio runtime join handle error occurred")]
    Schedule(#[from] JoinError),
    #[error("internal failure")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<warp::Error> for Error {
    fn from(e: warp::Error) -> Self {
        Error::Other(e.into())
    }
}
