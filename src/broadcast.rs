#![allow(dead_code)]
use crate::AwarenessRef;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::select;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use yrs::encoding::write::Write;
use yrs::sync::protocol::{MSG_SYNC, MSG_SYNC_UPDATE};
use yrs::sync::{DefaultProtocol, Error, Message, Protocol, SyncMessage};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::Update;

/// A broadcast group can be used to propagate updates produced by yrs [yrs::Doc] and [Awareness]
/// structures in a binary form that conforms to a y-sync protocol.
///
/// New receivers can subscribe to a broadcasting group via [BroadcastGroup::subscribe] method.
pub struct BroadcastGroup {
    awareness_sub: yrs::Subscription,
    doc_sub: yrs::Subscription,
    awareness_ref: AwarenessRef,
    sender: Sender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
    awareness_updater: JoinHandle<()>,
}

unsafe impl Send for BroadcastGroup {}
unsafe impl Sync for BroadcastGroup {}

impl BroadcastGroup {
    /// Creates a new [BroadcastGroup] over a provided `awareness` instance. All changes triggered
    /// by this awareness structure or its underlying document will be propagated to all subscribers
    /// which have been registered via [BroadcastGroup::subscribe] method.
    ///
    /// The overflow of the incoming events that needs to be propagates will be buffered up to a
    /// provided `buffer_capacity` size.
    pub async fn new(awareness: AwarenessRef, buffer_capacity: usize) -> Self {
        let (sender, receiver) = channel(buffer_capacity);
        let awareness_c = Arc::downgrade(&awareness);
        let mut lock = awareness.write().await;
        let sink = sender.clone();
        let doc_sub = {
            lock.doc_mut()
                .observe_update_v1(move |_txn, u| {
                    // we manually construct msg here to avoid update data copying
                    let mut encoder = EncoderV1::new();
                    encoder.write_var(MSG_SYNC);
                    encoder.write_var(MSG_SYNC_UPDATE);
                    encoder.write_buf(&u.update);
                    let msg = encoder.to_vec();
                    if let Err(_e) = sink.send(msg) {
                        // current broadcast group is being closed
                    }
                })
                .unwrap()
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = sender.clone();
        let awareness_sub = lock.on_update(move |e| {
            let added = e.added();
            let updated = e.updated();
            let removed = e.removed();
            let mut changed = Vec::with_capacity(added.len() + updated.len() + removed.len());
            changed.extend_from_slice(added);
            changed.extend_from_slice(updated);
            changed.extend_from_slice(removed);

            if let Err(_) = tx.send(changed) {
                tracing::warn!("failed to send awareness update");
            }
        });
        drop(lock);
        let awareness_updater = tokio::task::spawn(async move {
            while let Some(changed_clients) = rx.recv().await {
                if let Some(awareness) = awareness_c.upgrade() {
                    let awareness = awareness.read().await;
                    match awareness.update_with_clients(changed_clients) {
                        Ok(update) => {
                            if let Err(_) = sink.send(Message::Awareness(update).encode_v1()) {
                                tracing::warn!("couldn't broadcast awareness update");
                            }
                        }
                        Err(e) => {
                            tracing::warn!("error while computing awareness update: {}", e)
                        }
                    }
                } else {
                    return;
                }
            }
        });
        BroadcastGroup {
            awareness_ref: awareness,
            awareness_updater,
            sender,
            receiver,
            awareness_sub,
            doc_sub,
        }
    }

    /// Returns a reference to an underlying [Awareness] instance.
    pub fn awareness(&self) -> &AwarenessRef {
        &self.awareness_ref
    }

    /// Broadcasts user message to all active subscribers. Returns error if message could not have
    /// been broadcasted.
    pub fn broadcast(&self, msg: Vec<u8>) -> Result<(), SendError<Vec<u8>>> {
        self.sender.send(msg)?;
        Ok(())
    }

    /// Subscribes a new connection - represented by `sink`/`stream` pair implementing a futures
    /// Sink and Stream protocols - to a current broadcast group.
    ///
    /// Returns a subscription structure, which can be dropped in order to unsubscribe or awaited
    /// via [Subscription::completed] method in order to complete of its own volition (due to
    /// an internal connection error or closed connection).
    pub fn subscribe<Sink, Stream, E>(&self, sink: Arc<Mutex<Sink>>, stream: Stream) -> Subscription
    where
        Sink: SinkExt<Vec<u8>> + Send + Sync + Unpin + 'static,
        Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Sync + Unpin + 'static,
        <Sink as futures_util::Sink<Vec<u8>>>::Error: std::error::Error + Send + Sync,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.subscribe_with(sink, stream, DefaultProtocol)
    }

    /// Subscribes a new connection - represented by `sink`/`stream` pair implementing a futures
    /// Sink and Stream protocols - to a current broadcast group.
    ///
    /// Returns a subscription structure, which can be dropped in order to unsubscribe or awaited
    /// via [Subscription::completed] method in order to complete of its own volition (due to
    /// an internal connection error or closed connection).
    ///
    /// Unlike [BroadcastGroup::subscribe], this method can take [Protocol] parameter that allows to
    /// customize the y-sync protocol behavior.
    pub fn subscribe_with<Sink, Stream, E, P>(
        &self,
        sink: Arc<Mutex<Sink>>,
        mut stream: Stream,
        protocol: P,
    ) -> Subscription
    where
        Sink: SinkExt<Vec<u8>> + Send + Sync + Unpin + 'static,
        Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Sync + Unpin + 'static,
        <Sink as futures_util::Sink<Vec<u8>>>::Error: std::error::Error + Send + Sync,
        E: std::error::Error + Send + Sync + 'static,
        P: Protocol + Send + Sync + 'static,
    {
        let sink_task = {
            let sink = sink.clone();
            let mut receiver = self.sender.subscribe();
            tokio::spawn(async move {
                while let Ok(msg) = receiver.recv().await {
                    let mut sink = sink.lock().await;
                    if let Err(e) = sink.send(msg).await {
                        println!("broadcast failed to sent sync message");
                        return Err(Error::Other(Box::new(e)));
                    }
                }
                Ok(())
            })
        };
        let stream_task = {
            let awareness = self.awareness().clone();
            tokio::spawn(async move {
                while let Some(res) = stream.next().await {
                    let msg = Message::decode_v1(&res.map_err(|e| Error::Other(Box::new(e)))?)?;
                    let reply = Self::handle_msg(&protocol, &awareness, msg).await?;
                    match reply {
                        None => {}
                        Some(reply) => {
                            let mut sink = sink.lock().await;
                            sink.send(reply.encode_v1())
                                .await
                                .map_err(|e| Error::Other(Box::new(e)))?;
                        }
                    }
                }
                Ok(())
            })
        };

        Subscription {
            sink_task,
            stream_task,
        }
    }

    async fn handle_msg<P: Protocol>(
        protocol: &P,
        awareness: &AwarenessRef,
        msg: Message,
    ) -> Result<Option<Message>, Error> {
        match msg {
            Message::Sync(msg) => match msg {
                SyncMessage::SyncStep1(state_vector) => {
                    let awareness = awareness.read().await;
                    protocol.handle_sync_step1(&*awareness, state_vector)
                }
                SyncMessage::SyncStep2(update) => {
                    let mut awareness = awareness.write().await;
                    let update = Update::decode_v1(&update)?;
                    protocol.handle_sync_step2(&mut *awareness, update)
                }
                SyncMessage::Update(update) => {
                    let mut awareness = awareness.write().await;
                    let update = Update::decode_v1(&update)?;
                    protocol.handle_sync_step2(&mut *awareness, update)
                }
            },
            Message::Auth(deny_reason) => {
                let awareness = awareness.read().await;
                protocol.handle_auth(&*awareness, deny_reason)
            }
            Message::AwarenessQuery => {
                let awareness = awareness.read().await;
                protocol.handle_awareness_query(&*awareness)
            }
            Message::Awareness(update) => {
                let mut awareness = awareness.write().await;
                protocol.handle_awareness_update(&mut *awareness, update)
            }
            Message::Custom(tag, data) => {
                let mut awareness = awareness.write().await;
                protocol.missing_handle(&mut *awareness, tag, data)
            }
        }
    }
}

impl Drop for BroadcastGroup {
    fn drop(&mut self) {
        self.awareness_updater.abort();
    }
}

/// A subscription structure returned from [BroadcastGroup::subscribe], which represents a
/// subscribed connection. It can be dropped in order to unsubscribe or awaited via
/// [Subscription::completed] method in order to complete of its own volition (due to an internal
/// connection error or closed connection).
#[derive(Debug)]
pub struct Subscription {
    sink_task: JoinHandle<Result<(), Error>>,
    stream_task: JoinHandle<Result<(), Error>>,
}

impl Subscription {
    /// Consumes current subscription, waiting for it to complete. If an underlying connection was
    /// closed because of failure, an error which caused it to happen will be returned.
    ///
    /// This method doesn't invoke close procedure. If you need that, drop current subscription instead.
    pub async fn completed(self) -> Result<(), Error> {
        let res = select! {
            r1 = self.sink_task => r1,
            r2 = self.stream_task => r2,
        };
        res.map_err(|e| Error::Other(e.into()))?
    }
}

#[cfg(test)]
mod test {
    use crate::broadcast::BroadcastGroup;
    use futures_util::{ready, SinkExt, StreamExt};
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::sync::{Mutex, RwLock};
    use tokio_util::sync::PollSender;
    use yrs::sync::awareness::AwarenessUpdateEntry;
    use yrs::sync::{Awareness, AwarenessUpdate, Error, Message, SyncMessage};
    use yrs::updates::decoder::Decode;
    use yrs::updates::encoder::Encode;
    use yrs::{Doc, StateVector, Text, Transact};

    #[derive(Debug)]
    pub struct ReceiverStream<T> {
        inner: tokio::sync::mpsc::Receiver<T>,
    }

    impl<T> ReceiverStream<T> {
        /// Create a new `ReceiverStream`.
        pub fn new(recv: tokio::sync::mpsc::Receiver<T>) -> Self {
            Self { inner: recv }
        }
    }

    impl<T> futures_util::Stream for ReceiverStream<T> {
        type Item = Result<T, Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match ready!(self.inner.poll_recv(cx)) {
                None => Poll::Ready(None),
                Some(v) => Poll::Ready(Some(Ok(v))),
            }
        }
    }

    fn test_channel(capacity: usize) -> (PollSender<Vec<u8>>, ReceiverStream<Vec<u8>>) {
        let (s, r) = tokio::sync::mpsc::channel::<Vec<u8>>(capacity);
        let s = PollSender::new(s);
        let r = ReceiverStream::new(r);
        (s, r)
    }

    #[tokio::test]
    async fn broadcast_changes() -> Result<(), Box<dyn std::error::Error>> {
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");
        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let group = BroadcastGroup::new(awareness.clone(), 1).await;

        let (server_sender, mut client_receiver) = test_channel(1);
        let (mut client_sender, server_receiver) = test_channel(1);
        let _sub1 = group.subscribe(Arc::new(Mutex::new(server_sender)), server_receiver);

        // check update propagation
        {
            let a = awareness.write().await;
            text.push(&mut a.doc().transact_mut(), "a");
        }
        let msg = client_receiver.next().await;
        let msg = msg.map(|x| Message::decode_v1(&x.unwrap()).unwrap());
        assert_eq!(
            msg,
            Some(Message::Sync(SyncMessage::Update(vec![
                1, 1, 1, 0, 4, 1, 4, 116, 101, 115, 116, 1, 97, 0,
            ])))
        );

        // check awareness update propagation
        {
            let mut a = awareness.write().await;
            a.set_local_state(r#"{"key":"value"}"#)
        }

        let msg = client_receiver.next().await;
        let msg = msg.map(|x| Message::decode_v1(&x.unwrap()).unwrap());
        assert_eq!(
            msg,
            Some(Message::Awareness(AwarenessUpdate {
                clients: HashMap::from([(
                    1,
                    AwarenessUpdateEntry {
                        clock: 1,
                        json: r#"{"key":"value"}"#.to_string(),
                    },
                )]),
            }))
        );

        // check sync state request/response
        {
            client_sender
                .send(Message::Sync(SyncMessage::SyncStep1(StateVector::default())).encode_v1())
                .await?;
            let msg = client_receiver.next().await;
            let msg = msg.map(|x| Message::decode_v1(&x.unwrap()).unwrap());
            assert_eq!(
                msg,
                Some(Message::Sync(SyncMessage::SyncStep2(vec![
                    1, 1, 1, 0, 4, 1, 4, 116, 101, 115, 116, 1, 97, 0,
                ])))
            );
        }

        Ok(())
    }
}
