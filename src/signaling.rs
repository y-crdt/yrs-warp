use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;
use warp::ws::{Message, WebSocket};
use warp::Error;

const PING_TIMEOUT: Duration = Duration::from_secs(30);

/// Signaling service is used by y-webrtc protocol in order to exchange WebRTC offerings between
/// clients subscribing to particular rooms.
///
/// # Example
///
/// ```rust
/// use warp::{Filter, Rejection, Reply};
/// use warp::ws::{Ws, WebSocket};
/// use yrs_warp::signaling::{SignalingService, signaling_conn};
///
/// fn main() {
///   let signaling = SignalingService::new();
///   let ws = warp::path("signaling")
///       .and(warp::ws())
///       .and(warp::any().map(move || signaling.clone()))
///       .and_then(ws_handler);
///
///   //warp::serve(routes).run(([0, 0, 0, 0], 8000)).await;
/// }
///
/// async fn ws_handler(ws: Ws, svc: SignalingService) -> Result<impl Reply, Rejection> {
///   Ok(ws.on_upgrade(move |socket| peer(socket, svc)))
/// }
///
/// async fn peer(ws: WebSocket, svc: SignalingService) {
///   match signaling_conn(ws, svc).await {
///     Ok(_) => println!("signaling connection stopped"),
///     Err(e) => eprintln!("signaling connection failed: {}", e),
///   }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SignalingService(Topics);

impl SignalingService {
    pub fn new() -> Self {
        SignalingService(Arc::new(RwLock::new(Default::default())))
    }

    pub async fn publish(&self, topic: &str, msg: Message) -> Result<(), Error> {
        let mut failed = Vec::new();
        {
            let topics = self.0.read().await;
            if let Some(subs) = topics.get(topic) {
                let client_count = subs.len();
                tracing::info!("publishing message to {client_count} clients: {msg:?}");
                for sub in subs {
                    if let Err(e) = sub.try_send(msg.clone()).await {
                        tracing::info!("failed to send {msg:?}: {e}");
                        failed.push(sub.clone());
                    }
                }
            }
        }
        if !failed.is_empty() {
            let mut topics = self.0.write().await;
            if let Some(subs) = topics.get_mut(topic) {
                for f in failed {
                    subs.remove(&f);
                }
            }
        }
        Ok(())
    }

    pub async fn close_topic(&self, topic: &str) -> Result<(), Error> {
        let mut topics = self.0.write().await;
        if let Some(subs) = topics.remove(topic) {
            for sub in subs {
                if let Err(e) = sub.close().await {
                    tracing::warn!("failed to close connection on topic '{topic}': {e}");
                }
            }
        }
        Ok(())
    }

    pub async fn close(self) -> Result<(), Error> {
        let mut topics = self.0.write_owned().await;
        let mut all_conns = HashSet::new();
        for (_, subs) in topics.drain() {
            for sub in subs {
                all_conns.insert(sub);
            }
        }

        for conn in all_conns {
            if let Err(e) = conn.close().await {
                tracing::warn!("failed to close connection: {e}");
            }
        }

        Ok(())
    }
}

impl Default for SignalingService {
    fn default() -> Self {
        Self::new()
    }
}

type Topics = Arc<RwLock<HashMap<Arc<str>, HashSet<WsSink>>>>;

#[derive(Debug, Clone)]
struct WsSink(Arc<Mutex<SplitSink<WebSocket, Message>>>);

impl WsSink {
    fn new(sink: SplitSink<WebSocket, Message>) -> Self {
        WsSink(Arc::new(Mutex::new(sink)))
    }

    async fn try_send(&self, msg: Message) -> Result<(), Error> {
        let mut sink = self.0.lock().await;
        if let Err(e) = sink.send(msg).await {
            sink.close().await?;
            Err(e)
        } else {
            Ok(())
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let mut sink = self.0.lock().await;
        sink.close().await
    }
}

impl Hash for WsSink {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = Arc::as_ptr(&self.0) as usize;
        ptr.hash(state);
    }
}

impl PartialEq<Self> for WsSink {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for WsSink {}

/// Handle incoming signaling connection - it's a websocket connection used by y-webrtc protocol
/// to exchange offering metadata between y-webrtc peers. It also manages topic/room access.
pub async fn signaling_conn(ws: WebSocket, service: SignalingService) -> Result<(), Error> {
    let mut topics: Topics = service.0;
    let (sink, mut stream) = ws.split();
    let ws = WsSink::new(sink);
    let mut ping_interval = interval(PING_TIMEOUT);
    let mut state = ConnState::default();
    loop {
        select! {
            _ = ping_interval.tick() => {
                if !state.pong_received {
                    ws.close().await?;
                    drop(ping_interval);
                    return Ok(());
                } else {
                    state.pong_received = false;
                    if let Err(e) = ws.try_send(Message::ping(Vec::default())).await {
                        ws.close().await?;
                        return Err(e);
                    }
                }
            },
            res = stream.next() => {
                match res {
                    None => {
                        ws.close().await?;
                        return Ok(());
                    },
                    Some(Err(e)) => {
                        ws.close().await?;
                        return Err(e);
                    },
                    Some(Ok(msg)) => {
                        process_msg(msg, &ws, &mut state, &mut topics).await?;
                    }
                }
            }
        }
    }
}

const PING_MSG: &str = r#"{"type":"ping"}"#;
const PONG_MSG: &str = r#"{"type":"pong"}"#;

async fn process_msg(
    msg: Message,
    ws: &WsSink,
    state: &mut ConnState,
    topics: &mut Topics,
) -> Result<(), Error> {
    if msg.is_text() {
        let json = msg.to_str().unwrap();
        let msg = serde_json::from_str(json).unwrap();
        match msg {
            Signal::Subscribe {
                topics: topic_names,
            } => {
                if !topic_names.is_empty() {
                    let mut topics = topics.write().await;
                    for topic in topic_names {
                        tracing::trace!("subscribing new client to '{topic}'");
                        if let Some((key, _)) = topics.get_key_value(topic) {
                            state.subscribed_topics.insert(key.clone());
                            let subs = topics.get_mut(topic).unwrap();
                            subs.insert(ws.clone());
                        } else {
                            let topic: Arc<str> = topic.into();
                            state.subscribed_topics.insert(topic.clone());
                            let mut subs = HashSet::new();
                            subs.insert(ws.clone());
                            topics.insert(topic, subs);
                        };
                    }
                }
            }
            Signal::Unsubscribe {
                topics: topic_names,
            } => {
                if !topic_names.is_empty() {
                    let mut topics = topics.write().await;
                    for topic in topic_names {
                        if let Some(subs) = topics.get_mut(topic) {
                            tracing::trace!("unsubscribing client from '{topic}'");
                            subs.remove(ws);
                        }
                    }
                }
            }
            Signal::Publish { topic } => {
                let mut failed = Vec::new();
                {
                    let topics = topics.read().await;
                    if let Some(receivers) = topics.get(topic) {
                        let client_count = receivers.len();
                        tracing::trace!(
                            "publishing on {client_count} clients at '{topic}': {json}"
                        );
                        for receiver in receivers.iter() {
                            if let Err(e) = receiver.try_send(Message::text(json)).await {
                                tracing::info!(
                                    "failed to publish message {json} on '{topic}': {e}"
                                );
                                failed.push(receiver.clone());
                            }
                        }
                    }
                }
                if !failed.is_empty() {
                    let mut topics = topics.write().await;
                    if let Some(receivers) = topics.get_mut(topic) {
                        for f in failed {
                            receivers.remove(&f);
                        }
                    }
                }
            }
            Signal::Ping => {
                ws.try_send(Message::text(PONG_MSG)).await?;
            }
            Signal::Pong => {
                ws.try_send(Message::text(PING_MSG)).await?;
            }
        }
    } else if msg.is_close() {
        let mut topics = topics.write().await;
        for topic in state.subscribed_topics.drain() {
            if let Some(subs) = topics.get_mut(&topic) {
                subs.remove(ws);
                if subs.is_empty() {
                    topics.remove(&topic);
                }
            }
        }
        state.closed = true;
    } else if msg.is_ping() {
        ws.try_send(Message::ping(Vec::default())).await?;
    }
    Ok(())
}

#[derive(Debug)]
struct ConnState {
    closed: bool,
    pong_received: bool,
    subscribed_topics: HashSet<Arc<str>>,
}

impl Default for ConnState {
    fn default() -> Self {
        ConnState {
            closed: false,
            pong_received: true,
            subscribed_topics: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum Signal<'a> {
    #[serde(rename = "publish")]
    Publish { topic: &'a str },
    #[serde(rename = "subscribe")]
    Subscribe { topics: Vec<&'a str> },
    #[serde(rename = "unsubscribe")]
    Unsubscribe { topics: Vec<&'a str> },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "pong")]
    Pong,
}
