use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::ws::{WebSocket, Ws};
use warp::{Filter, Rejection, Reply};
use yrs::sync::Awareness;
use yrs::{Doc, Text, Transact};
use yrs_warp::broadcast::BroadcastGroup;
use yrs_warp::conn::Connection;
use yrs_warp::storage::kv::DocOps;
use yrs_warp::storage::sqlite::SqliteStore;
use yrs_warp::ws::{WarpSink, WarpStream};
use yrs_warp::AwarenessRef;

const STATIC_FILES_DIR: &str = "examples/code-mirror/frontend/dist";
const DB_PATH: &str = "examples/code-mirror/yrs.db";
const DOC_NAME: &str = "codemirror";

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber with more detailed logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .init();

    // Initialize SQLite store
    let store = Arc::new(SqliteStore::new(DB_PATH).expect("Failed to open SQLite database"));
    tracing::info!("SQLite store initialized at: {}", DB_PATH);

    // We're using a single static document shared among all the peers.
    let awareness: AwarenessRef = {
        let doc = Doc::new();

        // Load document state from storage
        {
            let mut txn = doc.transact_mut();
            match store.load_doc(DOC_NAME, &mut txn).await {
                Ok(_) => {
                    tracing::info!("Successfully loaded existing document from storage");
                }
                Err(e) => {
                    tracing::warn!("No existing document found or failed to load: {}", e);
                    // Initialize new document if no existing one found
                    let txt = doc.get_or_insert_text("codemirror");
                    txt.push(
                        &mut txn,
                        r#"function hello() {
  console.log('hello world');
}"#,
                    );
                    tracing::info!("Initialized new document with default content");
                }
            }
        }

        // Set up storage subscription for document updates
        let store_clone = store.clone();
        let _sub = doc.observe_update_v1(move |_, e| {
            let store = store_clone.clone();
            let update = e.update.clone();

            // Create a new blocking task to handle the update
            tokio::spawn(async move {
                tracing::debug!("Received document update of {} bytes", update.len());

                match store.push_update(DOC_NAME, &update).await {
                    Ok(_) => {
                        tracing::info!(
                            "Successfully stored update for document '{}', size: {} bytes",
                            DOC_NAME,
                            update.len()
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to store update for document '{}': {}",
                            DOC_NAME,
                            e
                        );
                    }
                }
            });
        });

        Arc::new(RwLock::new(Awareness::new(doc)))
    };

    // open a broadcast group that listens to awareness and document updates
    let bcast = Arc::new(BroadcastGroup::new(awareness.clone(), 32).await);
    tracing::info!("Broadcast group initialized");

    let static_files = warp::get().and(warp::fs::dir(STATIC_FILES_DIR));

    let ws = warp::path("my-room")
        .and(warp::ws())
        .and(warp::any().map(move || bcast.clone()))
        .and_then(ws_handler);

    let routes = ws.or(static_files);

    tracing::info!("Starting server on 0.0.0.0:8000");
    warp::serve(routes).run(([0, 0, 0, 0], 8000)).await;
}

async fn ws_handler(ws: Ws, bcast: Arc<BroadcastGroup>) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| peer(socket, bcast)))
}

async fn peer(ws: WebSocket, bcast: Arc<BroadcastGroup>) {
    let (ws_sink, ws_stream) = ws.split();
    let sink = WarpSink::from(ws_sink);
    let stream = WarpStream::from(ws_stream);

    let conn = Connection::new(bcast.awareness().clone(), sink, stream);
    tracing::debug!("New peer connection established");

    match conn.await {
        Ok(_) => tracing::info!("Peer connection finished successfully"),
        Err(e) => tracing::error!("Peer connection finished with error: {}", e),
    }
}
