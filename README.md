# Yrs web socket connections

This library is an extension over [Yjs](https://yjs.dev)/[Yrs](https://github.com/y-crdt/y-crdt) Conflict-Free Replicated Data Types (CRDT) message exchange protocol. It provides an utilities connect with Yjs web socket provider using Rust tokio's [warp](https://github.com/seanmonstar/warp) web server.

### Demo

A working demo can be seen under [examples](./examples) subfolder. It integrates this library API with Code Mirror 6, enhancing it with collaborative rich text document editing capabilities.

### Example

In order to gossip updates between different web socket connection from the clients collaborating over the same logical document, a broadcast group can be used:

```rust
#[tokio::main]
async fn main() {
    let awareness = Arc::new(RwLock::new(Awareness::new(Doc::new())));
    // open a broadcast group that listens to awareness and document updates
    // and has a pending message buffer of up to 32 updates
    let bcast = Arc::new(BroadcastGroup::open(awareness.clone(), 32).await);

    // pass dependencies to awareness and broadcast group to every new created connection
    let ws = warp::path("my-room")
        .and(warp::ws())
        .and(warp::any().map(move || awareness.clone()))
        .and(warp::any().map(move || bcast.clone()))
        .and_then(ws_handler);
    
    warp::serve(ws).run(([0, 0, 0, 0], 8000)).await;
}

async fn ws_handler(
    ws: Ws,
    awareness: AwarenessRef,
    bcast: Arc<BroadcastGroup>,
) -> Result<impl Reply, Rejection> {
    Ok(ws.on_upgrade(move |socket| peer(socket, awareness, bcast)))
}

async fn peer(ws: WebSocket, awareness: AwarenessRef, bcast: Arc<BroadcastGroup>) {
    // create new web socket connection wrapper that communicates via y-sync protocol
    let conn = WarpConn::new(awareness, ws);
    // subscribe a new connection to the broadcast group
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
```
