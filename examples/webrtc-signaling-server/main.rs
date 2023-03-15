use warp::ws::{WebSocket, Ws};
use warp::{Filter, Rejection, Reply};
use yrs_warp::signaling::{signaling_conn, SignalingService};

const STATIC_FILES_DIR: &str = "examples/webrtc-signaling-server/frontend/dist";

#[tokio::main]
async fn main() {
    let signaling = SignalingService::new();

    let static_files = warp::get().and(warp::fs::dir(STATIC_FILES_DIR));

    let ws = warp::path("signaling")
        .and(warp::ws())
        .and(warp::any().map(move || signaling.clone()))
        .and_then(ws_handler);

    let routes = ws.or(static_files);

    warp::serve(routes).run(([0, 0, 0, 0], 8000)).await;
}

async fn ws_handler(ws: Ws, svc: SignalingService) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| peer(socket, svc)))
}

async fn peer(ws: WebSocket, svc: SignalingService) {
    println!("new incoming signaling connection");
    match signaling_conn(ws, svc).await {
        Ok(_) => println!("signaling connection stopped"),
        Err(e) => eprintln!("signaling connection failed: {}", e),
    }
}
