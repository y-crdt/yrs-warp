use std::sync::Arc;
use tokio::select;
use tokio::sync::RwLock;
use warp::ws::{WebSocket, Ws};
use warp::{Filter, Rejection, Reply};
use y_sync::awareness::Awareness;
use yrs::{Doc, Text, Transact};
use yrs_warp::broadcast::BroadcastGroup;
use yrs_warp::ws::WarpConn;
use yrs_warp::AwarenessRef;

const STATIC_FILES_DIR: &str = "examples/code-mirror/frontend/dist";

#[tokio::main]
async fn main() {
    // We're using a single static document shared among all the peers.
    let awareness: AwarenessRef = {
        let doc = Doc::new();
        {
            // pre-initialize code mirror document with some text
            let txt = doc.get_or_insert_text("codemirror");
            let mut txn = doc.transact_mut();
            txt.push(
                &mut txn,
                r#"function hello() {
  console.log('hello world');
}"#,
            );
        }
        Arc::new(RwLock::new(Awareness::new(doc)))
    };

    // open a broadcast group that listens to awareness and document updates
    // and has a pending message buffer of up to 32 updates
    let bcast = Arc::new(BroadcastGroup::open(awareness.clone(), 32).await);

    let static_files = warp::get().and(warp::fs::dir(STATIC_FILES_DIR));

    let ws = warp::path("my-room")
        .and(warp::ws())
        .and(warp::any().map(move || awareness.clone()))
        .and(warp::any().map(move || bcast.clone()))
        .and_then(ws_handler);

    let routes = ws.or(static_files);

    warp::serve(routes).run(([0, 0, 0, 0], 8000)).await;
}

async fn ws_handler(
    ws: Ws,
    awareness: AwarenessRef,
    bcast: Arc<BroadcastGroup>,
) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| peer(socket, awareness, bcast)))
}

async fn peer(ws: WebSocket, awareness: AwarenessRef, bcast: Arc<BroadcastGroup>) {
    let conn = WarpConn::new(awareness, ws);
    let sub = bcast.join(conn.inbox().clone());
    select! {
        res = sub => {
            match res {
                Ok(_) => println!("broadcasting for channel finished successfully"),
                Err(e) => eprintln!("broadcasting for channel finished abruptly: {}", e),
            }
        }
        res = conn => {
            match res {
                Ok(_) => println!("peer disconnected"),
                Err(e) => eprintln!("peer error occurred: {}", e),
            }
        }
    }
}
