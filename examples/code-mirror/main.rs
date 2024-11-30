use futures_util::StreamExt;
use lmdb_rs::{core::DbCreate, Environment};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use warp::ws::{WebSocket, Ws};
use warp::{Filter, Rejection, Reply};
use yrs::sync::Awareness;
use yrs::{Doc, Text, Transact};
use yrs_warp::broadcast::BroadcastGroup;
use yrs_warp::conn::Connection;
use yrs_warp::storage::kv::DocOps;
use yrs_warp::storage::lmdb::LmdbStore;
use yrs_warp::ws::{WarpSink, WarpStream};
use yrs_warp::AwarenessRef;

const STATIC_FILES_DIR: &str = "examples/code-mirror/frontend/dist";
const DB_PATH: &str = "examples/code-mirror/db";
const DOC_NAME: &str = "codemirror";

#[tokio::main]
async fn main() {
    // Initialize LMDB environment
    let env = Arc::new(
        Environment::new()
            .autocreate_dir(true)
            .max_dbs(4)
            .open(DB_PATH, 0o777)
            .expect("Failed to open LMDB environment"),
    );
    let handle = Arc::new(
        env.create_db("yrs", DbCreate)
            .expect("Failed to create LMDB database"),
    );

    // We're using a single static document shared among all the peers.
    let awareness: AwarenessRef = {
        let doc = Doc::new();

        // Load document state from storage
        {
            let db_txn = env
                .get_reader()
                .expect("Failed to create reader transaction");
            let store = LmdbStore::from(db_txn.bind(&handle));
            let mut txn = doc.transact_mut();
            if let Err(e) = store.load_doc(DOC_NAME, &mut txn) {
                tracing::warn!("No existing document found or failed to load: {}", e);
                // Initialize new document if no existing one found
                let txt = doc.get_or_insert_text("codemirror");
                txt.push(
                    &mut txn,
                    r#"function hello() {
  console.log('hello world');
}"#,
                );
            }
        }

        // Set up storage subscription for document updates
        {
            let env = env.clone();
            let handle = handle.clone();
            let _sub = doc.observe_update_v1(move |_, e| {
                if let Ok(db_txn) = env.new_transaction() {
                    let store = LmdbStore::from(db_txn.bind(&handle));
                    if let Err(e) = store.push_update(DOC_NAME, &e.update) {
                        tracing::error!("Failed to store update: {}", e);
                        return;
                    }
                    if let Err(e) = db_txn.commit() {
                        tracing::error!("Failed to commit transaction: {}", e);
                    }
                }
            });
        }

        Arc::new(RwLock::new(Awareness::new(doc)))
    };

    // open a broadcast group that listens to awareness and document updates
    let bcast = Arc::new(BroadcastGroup::new(awareness.clone(), 32).await);

    let static_files = warp::get().and(warp::fs::dir(STATIC_FILES_DIR));

    let ws = warp::path("my-room")
        .and(warp::ws())
        .and(warp::any().map(move || bcast.clone()))
        .and_then(ws_handler);

    let routes = ws.or(static_files);

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

    match conn.await {
        Ok(_) => println!("broadcasting for channel finished successfully"),
        Err(e) => eprintln!("broadcasting for channel finished abruptly: {}", e),
    }
}
