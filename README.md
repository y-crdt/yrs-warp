# Yrs web socket connections

This library is an extension over [Yjs](https://yjs.dev)/[Yrs](https://github.com/y-crdt/y-crdt) Conflict-Free Replicated Data Types (CRDT) message exchange protocol. It provides an utilities connect with Yjs web socket provider using Rust tokio's [warp](https://github.com/seanmonstar/warp) web server.

### Basic example

```rust
#[tokio::main]
async fn main() {
    // We're using a single static document shared among all the peers.
    let awareness = Arc::new(RwLock::new(Awareness::new(Doc::new())));

    let ws = warp::path("my-room")
        .and(warp::ws())
        .and(warp::any().map(move || awareness.clone()))
        .and_then(ws_handler);

    warp::serve(ws).run(([0, 0, 0, 0], 8000)).await;
}

async fn ws_handler(ws: Ws, awareness: Arc<RwLock<Awareness>>) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| peer(socket, awareness)))
}

async fn peer(socket: WebSocket, awareness: Arc<RwLock<Awareness>>) {
    let conn = WarpConn::new(awareness, socket);
    match conn.await {
        Ok(()) => println!("peer disconnected"),
        Err(err) => eprintln!("peer error occurred: {}", err),
    }
}
```

### Demo

A working demo can be seen under [examples](./examples) subfolder. It integrates this library API with Code Mirror 6, enhancing it with collaborative rich text document editing capabilities.