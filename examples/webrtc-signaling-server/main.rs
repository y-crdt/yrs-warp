use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
    Router,
};
use tower_http::services::ServeDir;
use yrs_warp::signaling::{signaling_conn, SignalingService};

const STATIC_FILES_DIR: &str = "examples/webrtc-signaling-server/frontend/dist";

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .init();

    let signaling = SignalingService::new();
    tracing::info!("SignalingService initialized");

    let app = Router::new()
        .route("/signaling", get(ws_handler))
        .nest_service("/", ServeDir::new(STATIC_FILES_DIR))
        .with_state(signaling);

    tracing::info!("Starting server on 0.0.0.0:8000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    axum::extract::State(svc): axum::extract::State<SignalingService>,
) -> Response {
    ws.on_upgrade(move |socket| peer(socket, svc))
}

async fn peer(ws: WebSocket, svc: SignalingService) {
    tracing::info!("New incoming signaling connection");
    match signaling_conn(ws, svc).await {
        Ok(_) => tracing::info!("Signaling connection stopped"),
        Err(e) => tracing::error!("Signaling connection failed: {}", e),
    }
}
