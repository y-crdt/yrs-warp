#![allow(dead_code)]
use futures_util::sink::SinkExt;
use futures_util::StreamExt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use tokio::spawn;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use yrs::encoding::read::Cursor;
use yrs::sync::Awareness;
use yrs::sync::{DefaultProtocol, Error, Message, MessageReader, Protocol, SyncMessage};
use yrs::updates::decoder::{Decode, DecoderV1};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::Update;

/// Connection handler over a pair of message streams, which implements a Yjs/Yrs awareness and
/// update exchange protocol.
///
/// This connection implements Future pattern and can be awaited upon in order for a caller to
/// recognize whether underlying websocket connection has been finished gracefully or abruptly.
#[derive(Debug)]
pub struct Connection<Sink, Stream> {
    processing_loop: JoinHandle<Result<(), Error>>,
    awareness: Arc<RwLock<Awareness>>,
    inbox: Arc<Mutex<Sink>>,
    _stream: PhantomData<Stream>,
}

impl<Sink, Stream, E> Connection<Sink, Stream>
where
    Sink: SinkExt<Vec<u8>, Error = E> + Send + Sync + Unpin + 'static,
    E: Into<Error> + Send + Sync,
{
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), Error> {
        let mut inbox = self.inbox.lock().await;
        match inbox.send(msg).await {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn close(self) -> Result<(), E> {
        let mut inbox = self.inbox.lock().await;
        inbox.close().await
    }

    pub fn sink(&self) -> Weak<Mutex<Sink>> {
        Arc::downgrade(&self.inbox)
    }
}

impl<Sink, Stream, E> Connection<Sink, Stream>
where
    Stream: StreamExt<Item = Result<Vec<u8>, E>> + Send + Sync + Unpin + 'static,
    Sink: SinkExt<Vec<u8>, Error = E> + Send + Sync + Unpin + 'static,
    E: Into<Error> + Send + Sync,
{
    /// Wraps incoming [WebSocket] connection and supplied [Awareness] accessor into a new
    /// connection handler capable of exchanging Yrs/Yjs messages.
    ///
    /// While creation of new [WarpConn] always succeeds, a connection itself can possibly fail
    /// while processing incoming input/output. This can be detected by awaiting for returned
    /// [WarpConn] and handling the awaited result.
    pub fn new(awareness: Arc<RwLock<Awareness>>, sink: Sink, stream: Stream) -> Self {
        Self::with_protocol(awareness, sink, stream, DefaultProtocol)
    }

    /// Returns an underlying [Awareness] structure, that contains client state of that connection.
    pub fn awareness(&self) -> &Arc<RwLock<Awareness>> {
        &self.awareness
    }

    /// Wraps incoming [WebSocket] connection and supplied [Awareness] accessor into a new
    /// connection handler capable of exchanging Yrs/Yjs messages.
    ///
    /// While creation of new [WarpConn] always succeeds, a connection itself can possibly fail
    /// while processing incoming input/output. This can be detected by awaiting for returned
    /// [WarpConn] and handling the awaited result.
    pub fn with_protocol<P>(
        awareness: Arc<RwLock<Awareness>>,
        sink: Sink,
        mut stream: Stream,
        protocol: P,
    ) -> Self
    where
        P: Protocol + Send + Sync + 'static,
    {
        let sink = Arc::new(Mutex::new(sink));
        let inbox = sink.clone();
        let loop_sink = Arc::downgrade(&sink);
        let loop_awareness = Arc::downgrade(&awareness);
        let processing_loop: JoinHandle<Result<(), Error>> = spawn(async move {
            // at the beginning send SyncStep1 and AwarenessUpdate
            let payload = {
                let awareness = loop_awareness.upgrade().unwrap();
                let mut encoder = EncoderV1::new();
                let awareness = awareness.read().await;
                protocol.start(&awareness, &mut encoder)?;
                encoder.to_vec()
            };
            if !payload.is_empty() {
                if let Some(sink) = loop_sink.upgrade() {
                    let mut s = sink.lock().await;
                    if let Err(e) = s.send(payload).await {
                        return Err(e.into());
                    }
                } else {
                    return Ok(()); // parent ConnHandler has been dropped
                }
            }

            while let Some(input) = stream.next().await {
                match input {
                    Ok(data) => {
                        if let Some(mut sink) = loop_sink.upgrade() {
                            if let Some(awareness) = loop_awareness.upgrade() {
                                match Self::process(&protocol, &awareness, &mut sink, data).await {
                                    Ok(()) => { /* continue */ }
                                    Err(e) => {
                                        return Err(e);
                                    }
                                }
                            } else {
                                return Ok(()); // parent ConnHandler has been dropped
                            }
                        } else {
                            return Ok(()); // parent ConnHandler has been dropped
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            Ok(())
        });
        Connection {
            processing_loop,
            awareness,
            inbox,
            _stream: PhantomData::default(),
        }
    }

    async fn process<P: Protocol>(
        protocol: &P,
        awareness: &Arc<RwLock<Awareness>>,
        sink: &mut Arc<Mutex<Sink>>,
        input: Vec<u8>,
    ) -> Result<(), Error> {
        let mut decoder = DecoderV1::new(Cursor::new(&input));
        let reader = MessageReader::new(&mut decoder);
        for r in reader {
            let msg = r?;
            if let Some(reply) = handle_msg(protocol, &awareness, msg).await? {
                let mut sender = sink.lock().await;
                if let Err(e) = sender.send(reply.encode_v1()).await {
                    println!("connection failed to send back the reply");
                    return Err(e.into());
                } else {
                    println!("connection send back the reply");
                }
            }
        }
        Ok(())
    }
}

impl<Sink, Stream> Unpin for Connection<Sink, Stream> {}

impl<Sink, Stream> Future for Connection<Sink, Stream> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.processing_loop).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Other(e.into()))),
            Poll::Ready(Ok(r)) => Poll::Ready(r),
        }
    }
}

pub async fn handle_msg<P: Protocol>(
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

#[cfg(test)]
mod test {
    use crate::broadcast::BroadcastGroup;
    use crate::conn::Connection;
    use bytes::{Bytes, BytesMut};
    use futures_util::SinkExt;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::{TcpListener, TcpSocket};
    use tokio::sync::{Mutex, Notify, RwLock};
    use tokio::task;
    use tokio::task::JoinHandle;
    use tokio::time::{sleep, timeout};
    use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite, LengthDelimitedCodec};
    use yrs::sync::{Awareness, Error, Message, SyncMessage};
    use yrs::updates::encoder::Encode;
    use yrs::{Doc, GetString, Subscription, Text, Transact};

    #[derive(Debug, Default)]
    struct YrsCodec(LengthDelimitedCodec);

    impl Encoder<Vec<u8>> for YrsCodec {
        type Error = Error;

        fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
            self.0.encode(Bytes::from(item), dst)?;
            Ok(())
        }
    }

    impl Decoder for YrsCodec {
        type Item = Vec<u8>;
        type Error = Error;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if let Some(bytes) = self.0.decode(src)? {
                Ok(Some(bytes.freeze().to_vec()))
            } else {
                Ok(None)
            }
        }
    }

    type WrappedStream = FramedRead<OwnedReadHalf, YrsCodec>;
    type WrappedSink = FramedWrite<OwnedWriteHalf, YrsCodec>;

    async fn start_server(
        addr: SocketAddr,
        bcast: BroadcastGroup,
    ) -> Result<JoinHandle<()>, Box<dyn std::error::Error>> {
        let server = TcpListener::bind(addr).await?;
        Ok(tokio::spawn(async move {
            let mut subscribers = Vec::new();
            while let Ok((stream, _)) = server.accept().await {
                let (reader, writer) = stream.into_split();
                let stream = WrappedStream::new(reader, YrsCodec::default());
                let sink = WrappedSink::new(writer, YrsCodec::default());
                let sub = bcast.subscribe(Arc::new(Mutex::new(sink)), stream);
                subscribers.push(sub);
            }
        }))
    }

    async fn client(
        addr: SocketAddr,
        doc: Doc,
    ) -> Result<Connection<WrappedSink, WrappedStream>, Box<dyn std::error::Error>> {
        let stream = TcpSocket::new_v4()?.connect(addr).await?;
        let (reader, writer) = stream.into_split();
        let stream: WrappedStream = WrappedStream::new(reader, YrsCodec::default());
        let sink: WrappedSink = WrappedSink::new(writer, YrsCodec::default());
        Ok(Connection::new(
            Arc::new(RwLock::new(Awareness::new(doc))),
            sink,
            stream,
        ))
    }

    fn create_notifier(doc: &Doc) -> (Arc<Notify>, Subscription) {
        let n = Arc::new(Notify::new());
        let sub = {
            let n = n.clone();
            doc.observe_update_v1(move |_, _| n.notify_waiters())
                .unwrap()
        };
        (n, sub)
    }

    const TIMEOUT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn change_introduced_by_server_reaches_subscribed_clients(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = SocketAddr::from_str("127.0.0.1:6600").unwrap();
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");
        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let _server = start_server(server_addr.clone(), bcast).await?;

        let doc = Doc::new();
        let (n, _sub) = create_notifier(&doc);
        let c1 = client(server_addr.clone(), doc).await?;

        {
            let lock = awareness.write().await;
            text.push(&mut lock.doc().transact_mut(), "abc");
        }

        timeout(TIMEOUT, n.notified()).await?;

        {
            let awareness = c1.awareness().read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn subscribed_client_fetches_initial_state() -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = SocketAddr::from_str("127.0.0.1:6601").unwrap();
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");

        text.push(&mut doc.transact_mut(), "abc");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let _server = start_server(server_addr.clone(), bcast).await?;

        let doc = Doc::new();
        let (n, _sub) = create_notifier(&doc);
        let c1 = client(server_addr.clone(), doc).await?;

        timeout(TIMEOUT, n.notified()).await?;

        {
            let awareness = c1.awareness().read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn changes_from_one_client_reach_others() -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = SocketAddr::from_str("127.0.0.1:6602").unwrap();
        let doc = Doc::with_client_id(1);
        let _text = doc.get_or_insert_text("test");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let _server = start_server(server_addr.clone(), bcast).await?;

        let d1 = Doc::with_client_id(2);
        let c1 = client(server_addr.clone(), d1).await?;
        // by default changes made by document on the client side are not propagated automatically
        let _sub11 = {
            let sink = c1.sink();
            let a = c1.awareness().write().await;
            let doc = a.doc();
            doc.observe_update_v1(move |_, e| {
                let update = e.update.to_owned();
                if let Some(sink) = sink.upgrade() {
                    task::spawn(async move {
                        let msg = Message::Sync(SyncMessage::Update(update)).encode_v1();
                        let mut sink = sink.lock().await;
                        sink.send(msg).await.unwrap();
                    });
                }
            })
            .unwrap()
        };

        let d2 = Doc::with_client_id(3);
        let (n2, _sub2) = create_notifier(&d2);
        let c2 = client(server_addr.clone(), d2).await?;

        {
            let a = c1.awareness().write().await;
            let doc = a.doc();
            let text = doc.get_or_insert_text("test");
            text.push(&mut doc.transact_mut(), "def");
        }

        timeout(TIMEOUT, n2.notified()).await?;

        {
            let awareness = c2.awareness.read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "def".to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn client_failure_doesnt_affect_others() -> Result<(), Box<dyn std::error::Error>> {
        let server_addr = SocketAddr::from_str("127.0.0.1:6604").unwrap();
        let doc = Doc::with_client_id(1);
        let _ = doc.get_or_insert_text("test");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let _server = start_server(server_addr.clone(), bcast).await?;

        let d1 = Doc::with_client_id(2);
        let c1 = client(server_addr.clone(), d1).await?;
        // by default changes made by document on the client side are not propagated automatically
        let _sub11 = {
            let sink = c1.sink();
            let a = c1.awareness().write().await;
            let doc = a.doc();
            doc.observe_update_v1(move |_, e| {
                let update = e.update.to_owned();
                if let Some(sink) = sink.upgrade() {
                    task::spawn(async move {
                        let msg = Message::Sync(SyncMessage::Update(update)).encode_v1();
                        let mut sink = sink.lock().await;
                        sink.send(msg).await.unwrap();
                    });
                }
            })
            .unwrap()
        };

        let d2 = Doc::with_client_id(3);
        let (n2, sub2) = create_notifier(&d2);
        let c2 = client(server_addr.clone(), d2).await?;

        let d3 = Doc::with_client_id(4);
        let (n3, sub3) = create_notifier(&d3);
        let c3 = client(server_addr.clone(), d3).await?;

        {
            let a = c1.awareness().write().await;
            let doc = a.doc();
            let text = doc.get_or_insert_text("test");
            text.push(&mut doc.transact_mut(), "abc");
        }

        // on the first try both C2 and C3 should receive the update
        //timeout(TIMEOUT, n2.notified()).await.unwrap();
        //timeout(TIMEOUT, n3.notified()).await.unwrap();
        sleep(TIMEOUT).await;

        {
            let awareness = c2.awareness.read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }
        {
            let awareness = c3.awareness.read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }

        // drop client, causing abrupt ending
        drop(c3);
        drop(n3);
        drop(sub3);
        // C2 notification subscription has been realized, we need to refresh it
        drop(n2);
        drop(sub2);

        let (n2, _sub2) = {
            let a = c2.awareness().write().await;
            let doc = a.doc();
            create_notifier(doc)
        };

        {
            let a = c1.awareness().write().await;
            let doc = a.doc();
            let text = doc.get_or_insert_text("test");
            text.push(&mut doc.transact_mut(), "def");
        }

        timeout(TIMEOUT, n2.notified()).await.unwrap();

        {
            let awareness = c2.awareness.read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abcdef".to_string());
        }

        Ok(())
    }
}
