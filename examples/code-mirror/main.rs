use std::sync::Arc;
use tokio::sync::RwLock;
use warp::ws::{WebSocket, Ws};
use warp::{Filter, Rejection, Reply};
use yrs::Doc;
use yrs_ws::awareness::Awareness;
use yrs_ws::ws::WarpConn;

type AwarenessRef = Arc<RwLock<Awareness>>;

const STATIC_FILES_DIR: &str = "examples/code-mirror/frontend/dist";

#[tokio::main]
async fn main() {
    // We're using a single static document shared among all the peers.
    let awareness: AwarenessRef = {
        let doc = Doc::new();
        {
            // pre-initialize code mirror document with some text
            let mut txn = doc.transact();
            let txt = txn.get_text("codemirror");
            txt.push(
                &mut txn,
                r#"function hello() {
  console.log('hello world');
}"#,
            );
        }
        Arc::new(RwLock::new(Awareness::new(doc)))
    };

    let static_files = warp::get().and(warp::fs::dir(STATIC_FILES_DIR));

    let ws = warp::path("my-room")
        .and(warp::ws())
        .and(warp::any().map(move || awareness.clone()))
        .and_then(ws_handler);

    let routes = ws.or(static_files);

    warp::serve(routes).run(([0, 0, 0, 0], 8000)).await;
}

async fn ws_handler(ws: Ws, awareness: AwarenessRef) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| peer(socket, awareness)))
}

async fn peer(ws: WebSocket, awareness: AwarenessRef) {
    let conn = WarpConn::new(awareness, ws);
    match conn.await {
        Ok(()) => {
            println!("peer disconnected");
        }
        Err(err) => {
            eprintln!("peer error occurred: {}", err);
        }
    }
}
