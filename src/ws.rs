use crate::conn::Connection;
use crate::AwarenessRef;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use warp::ws::{Message, WebSocket};
use yrs::sync::Error;

/// Connection Wrapper over a [WebSocket], which implements a Yjs/Yrs awareness and update exchange
/// protocol.
///
/// This connection implements Future pattern and can be awaited upon in order for a caller to
/// recognize whether underlying websocket connection has been finished gracefully or abruptly.
#[repr(transparent)]
#[derive(Debug)]
pub struct WarpConn(Connection<WarpSink, WarpStream>);

impl WarpConn {
    pub fn new(awareness: AwarenessRef, socket: WebSocket) -> Self {
        let (sink, stream) = socket.split();
        let conn = Connection::new(awareness, WarpSink(sink), WarpStream(stream));
        WarpConn(conn)
    }
}

impl core::future::Future for WarpConn {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Other(e.into()))),
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
        }
    }
}

/// A warp websocket sink wrapper, that implements futures `Sink` in a way, that makes it compatible
/// with y-sync protocol, so that it can be used by y-sync crate [BroadcastGroup].
///
/// # Examples
///
/// ```rust
/// use std::net::SocketAddr;
/// use std::str::FromStr;
/// use std::sync::Arc;
/// use futures_util::StreamExt;
/// use tokio::sync::Mutex;
/// use tokio::task::JoinHandle;
/// use warp::{Filter, Rejection, Reply};
/// use warp::ws::{WebSocket, Ws};
/// use yrs_warp::broadcast::BroadcastGroup;
/// use yrs_warp::ws::{WarpSink, WarpStream};
///
/// async fn start_server(
///     addr: &str,
///     bcast: Arc<BroadcastGroup>,
/// ) -> Result<JoinHandle<()>, Box<dyn std::error::Error>> {
///     let addr = SocketAddr::from_str(addr)?;
///     let ws = warp::path("my-room")
///         .and(warp::ws())
///         .and(warp::any().map(move || bcast.clone()))
///         .and_then(ws_handler);
///
///     Ok(tokio::spawn(async move {
///         warp::serve(ws).run(addr).await;
///     }))
/// }
///
/// async fn ws_handler(ws: Ws, bcast: Arc<BroadcastGroup>) -> Result<impl Reply, Rejection> {
///     Ok(ws.on_upgrade(move |socket| peer(socket, bcast)))
/// }
///
/// async fn peer(ws: WebSocket, bcast: Arc<BroadcastGroup>) {
///     let (sink, stream) = ws.split();
///     // convert warp web socket into compatible sink/stream
///     let sink = Arc::new(Mutex::new(WarpSink::from(sink)));
///     let stream = WarpStream::from(stream);
///     // subscribe to broadcast group
///     let sub = bcast.subscribe(sink, stream);
///     // wait for subscribed connection to close itself
///     match sub.completed().await {
///         Ok(_) => println!("broadcasting for channel finished successfully"),
///         Err(e) => eprintln!("broadcasting for channel finished abruptly: {}", e),
///     }
/// }
/// ```
#[repr(transparent)]
#[derive(Debug)]
pub struct WarpSink(SplitSink<WebSocket, Message>);

impl From<SplitSink<WebSocket, Message>> for WarpSink {
    fn from(sink: SplitSink<WebSocket, Message>) -> Self {
        WarpSink(sink)
    }
}

impl Into<SplitSink<WebSocket, Message>> for WarpSink {
    fn into(self) -> SplitSink<WebSocket, Message> {
        self.0
    }
}

impl futures_util::Sink<Vec<u8>> for WarpSink {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match Pin::new(&mut self.0).poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Other(e.into()))),
            Poll::Ready(_) => Poll::Ready(Ok(())),
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        if let Err(e) = Pin::new(&mut self.0).start_send(Message::binary(item)) {
            Err(Error::Other(e.into()))
        } else {
            Ok(())
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match Pin::new(&mut self.0).poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Other(e.into()))),
            Poll::Ready(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match Pin::new(&mut self.0).poll_close(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Other(e.into()))),
            Poll::Ready(_) => Poll::Ready(Ok(())),
        }
    }
}

/// A warp websocket stream wrapper, that implements futures `Stream` in a way, that makes it compatible
/// with y-sync protocol, so that it can be used by y-sync crate [BroadcastGroup].
///
/// # Examples
///
/// ```rust
/// use std::net::SocketAddr;
/// use std::str::FromStr;
/// use std::sync::Arc;
/// use futures_util::StreamExt;
/// use tokio::sync::Mutex;
/// use tokio::task::JoinHandle;
/// use warp::{Filter, Rejection, Reply};
/// use warp::ws::{WebSocket, Ws};
/// use yrs_warp::broadcast::BroadcastGroup;
/// use yrs_warp::ws::{WarpSink, WarpStream};
///
/// async fn start_server(
///     addr: &str,
///     bcast: Arc<BroadcastGroup>,
/// ) -> Result<JoinHandle<()>, Box<dyn std::error::Error>> {
///     let addr = SocketAddr::from_str(addr)?;
///     let ws = warp::path("my-room")
///         .and(warp::ws())
///         .and(warp::any().map(move || bcast.clone()))
///         .and_then(ws_handler);
///
///     Ok(tokio::spawn(async move {
///         warp::serve(ws).run(addr).await;
///     }))
/// }
///
/// async fn ws_handler(ws: Ws, bcast: Arc<BroadcastGroup>) -> Result<impl Reply, Rejection> {
///     Ok(ws.on_upgrade(move |socket| peer(socket, bcast)))
/// }
///
/// async fn peer(ws: WebSocket, bcast: Arc<BroadcastGroup>) {
///     let (sink, stream) = ws.split();
///     // convert warp web socket into compatible sink/stream
///     let sink = Arc::new(Mutex::new(WarpSink::from(sink)));
///     let stream = WarpStream::from(stream);
///     // subscribe to broadcast group
///     let sub = bcast.subscribe(sink, stream);
///     // wait for subscribed connection to close itself
///     match sub.completed().await {
///         Ok(_) => println!("broadcasting for channel finished successfully"),
///         Err(e) => eprintln!("broadcasting for channel finished abruptly: {}", e),
///     }
/// }
/// ```
#[derive(Debug)]
pub struct WarpStream(SplitStream<WebSocket>);

impl From<SplitStream<WebSocket>> for WarpStream {
    fn from(stream: SplitStream<WebSocket>) -> Self {
        WarpStream(stream)
    }
}

impl Into<SplitStream<WebSocket>> for WarpStream {
    fn into(self) -> SplitStream<WebSocket> {
        self.0
    }
}

impl Stream for WarpStream {
    type Item = Result<Vec<u8>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(res)) => match res {
                Ok(item) => Poll::Ready(Some(Ok(item.into_bytes()))),
                Err(e) => Poll::Ready(Some(Err(Error::Other(e.into())))),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use crate::broadcast::BroadcastGroup;
    use crate::conn::Connection;
    use crate::ws::{WarpSink, WarpStream};
    use futures_util::stream::{SplitSink, SplitStream};
    use futures_util::{ready, SinkExt, Stream, StreamExt};
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::sync::{Mutex, Notify, RwLock};
    use tokio::task;
    use tokio::task::JoinHandle;
    use tokio::time::{sleep, timeout};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
    use warp::ws::{WebSocket, Ws};
    use warp::{Filter, Rejection, Reply, Sink};
    use yrs::sync::{Awareness, Error};
    use yrs::updates::encoder::Encode;
    use yrs::{Doc, GetString, Subscription, Text, Transact};

    async fn start_server(
        addr: &str,
        bcast: Arc<BroadcastGroup>,
    ) -> Result<JoinHandle<()>, Box<dyn std::error::Error>> {
        let addr = SocketAddr::from_str(addr)?;
        let ws = warp::path("my-room")
            .and(warp::ws())
            .and(warp::any().map(move || bcast.clone()))
            .and_then(ws_handler);

        Ok(tokio::spawn(async move {
            warp::serve(ws).run(addr).await;
        }))
    }

    async fn ws_handler(ws: Ws, bcast: Arc<BroadcastGroup>) -> Result<impl Reply, Rejection> {
        Ok(ws.on_upgrade(move |socket| peer(socket, bcast)))
    }

    async fn peer(ws: WebSocket, bcast: Arc<BroadcastGroup>) {
        let (sink, stream) = ws.split();
        let sink = Arc::new(Mutex::new(WarpSink::from(sink)));
        let stream = WarpStream::from(stream);
        let sub = bcast.subscribe(sink, stream);
        match sub.completed().await {
            Ok(_) => println!("broadcasting for channel finished successfully"),
            Err(e) => eprintln!("broadcasting for channel finished abruptly: {}", e),
        }
    }

    struct TungsteniteSink(SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>);

    impl Sink<Vec<u8>> for TungsteniteSink {
        type Error = Error;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let sink = unsafe { Pin::new_unchecked(&mut self.0) };
            let result = ready!(sink.poll_ready(cx));
            match result {
                Ok(_) => Poll::Ready(Ok(())),
                Err(e) => Poll::Ready(Err(Error::Other(Box::new(e)))),
            }
        }

        fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
            let sink = unsafe { Pin::new_unchecked(&mut self.0) };
            let result = sink.start_send(Message::binary(item));
            match result {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Other(Box::new(e))),
            }
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let sink = unsafe { Pin::new_unchecked(&mut self.0) };
            let result = ready!(sink.poll_flush(cx));
            match result {
                Ok(_) => Poll::Ready(Ok(())),
                Err(e) => Poll::Ready(Err(Error::Other(Box::new(e)))),
            }
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            let sink = unsafe { Pin::new_unchecked(&mut self.0) };
            let result = ready!(sink.poll_close(cx));
            match result {
                Ok(_) => Poll::Ready(Ok(())),
                Err(e) => Poll::Ready(Err(Error::Other(Box::new(e)))),
            }
        }
    }

    struct TungsteniteStream(SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>);
    impl Stream for TungsteniteStream {
        type Item = Result<Vec<u8>, Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let stream = unsafe { Pin::new_unchecked(&mut self.0) };
            let result = ready!(stream.poll_next(cx));
            match result {
                None => Poll::Ready(None),
                Some(Ok(msg)) => Poll::Ready(Some(Ok(msg.into_data()))),
                Some(Err(e)) => Poll::Ready(Some(Err(Error::Other(Box::new(e))))),
            }
        }
    }

    async fn client(
        addr: &str,
        doc: Doc,
    ) -> Result<Connection<TungsteniteSink, TungsteniteStream>, Box<dyn std::error::Error>> {
        let (stream, _) = tokio_tungstenite::connect_async(addr).await?;
        let (sink, stream) = stream.split();
        let sink = TungsteniteSink(sink);
        let stream = TungsteniteStream(stream);
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
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");
        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let server = start_server("0.0.0.0:6600", Arc::new(bcast)).await?;

        let doc = Doc::new();
        let (n, sub) = create_notifier(&doc);
        let c1 = client("ws://localhost:6600/my-room", doc).await?;

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
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");

        text.push(&mut doc.transact_mut(), "abc");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let server = start_server("0.0.0.0:6601", Arc::new(bcast)).await?;

        let doc = Doc::new();
        let (n, sub) = create_notifier(&doc);
        let c1 = client("ws://localhost:6601/my-room", doc).await?;

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
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let server = start_server("0.0.0.0:6602", Arc::new(bcast)).await?;

        let d1 = Doc::with_client_id(2);
        let c1 = client("ws://localhost:6602/my-room", d1).await?;
        // by default changes made by document on the client side are not propagated automatically
        let sub11 = {
            let sink = c1.sink();
            let a = c1.awareness().write().await;
            let doc = a.doc();
            doc.observe_update_v1(move |txn, e| {
                let update = e.update.to_owned();
                if let Some(sink) = sink.upgrade() {
                    task::spawn(async move {
                        let msg = yrs::sync::Message::Sync(yrs::sync::SyncMessage::Update(update))
                            .encode_v1();
                        let mut sink = sink.lock().await;
                        sink.send(msg).await.unwrap();
                    });
                }
            })
            .unwrap()
        };

        let d2 = Doc::with_client_id(3);
        let (n2, sub2) = create_notifier(&d2);
        let c2 = client("ws://localhost:6602/my-room", d2).await?;

        {
            let a = c1.awareness().write().await;
            let doc = a.doc();
            let text = doc.get_or_insert_text("test");
            text.push(&mut doc.transact_mut(), "def");
        }

        timeout(TIMEOUT, n2.notified()).await?;

        {
            let awareness = c2.awareness().read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "def".to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn client_failure_doesnt_affect_others() -> Result<(), Box<dyn std::error::Error>> {
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("test");

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let bcast = BroadcastGroup::new(awareness.clone(), 10).await;
        let server = start_server("0.0.0.0:6603", Arc::new(bcast)).await?;

        let d1 = Doc::with_client_id(2);
        let c1 = client("ws://localhost:6603/my-room", d1).await?;
        // by default changes made by document on the client side are not propagated automatically
        let sub11 = {
            let sink = c1.sink();
            let a = c1.awareness().write().await;
            let doc = a.doc();
            doc.observe_update_v1(move |txn, e| {
                let update = e.update.to_owned();
                if let Some(sink) = sink.upgrade() {
                    task::spawn(async move {
                        let msg = yrs::sync::Message::Sync(yrs::sync::SyncMessage::Update(update))
                            .encode_v1();
                        let mut sink = sink.lock().await;
                        sink.send(msg).await.unwrap();
                    });
                }
            })
            .unwrap()
        };

        let d2 = Doc::with_client_id(3);
        let (n2, sub2) = create_notifier(&d2);
        let c2 = client("ws://localhost:6603/my-room", d2).await?;

        let d3 = Doc::with_client_id(4);
        let (n3, sub3) = create_notifier(&d3);
        let c3 = client("ws://localhost:6603/my-room", d3).await?;

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
            let awareness = c2.awareness().read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abc".to_string());
        }
        {
            let awareness = c3.awareness().read().await;
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

        let (n2, sub2) = {
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
            let awareness = c2.awareness().read().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("test");
            let str = text.get_string(&doc.transact());
            assert_eq!(str, "abcdef".to_string());
        }

        Ok(())
    }
}
