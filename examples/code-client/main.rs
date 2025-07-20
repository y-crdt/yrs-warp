use crossterm::event::{self, Event, KeyCode};
use crossterm::style::{
    Attribute, Color, Print, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
};
use crossterm::terminal::{self, ClearType};
use crossterm::{cursor, execute, queue};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{stdout, Write};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message as TokioMessage;
use yrs::sync::{Awareness, Message, SyncMessage};
use yrs::updates::encoder::Encode;
use yrs::{Assoc, Doc, GetString, IndexedSequence, StickyIndex, Text, Transact, ID};
use yrs_warp::conn::Connection;

#[allow(dead_code)]
#[derive(Deserialize, Serialize)]
struct State {
    user: StateUser,
    cursor: StateCursor,
}

#[allow(non_snake_case, dead_code)]
#[derive(Deserialize, Serialize)]
struct StateUser {
    name: String,
    color: String,
    colorLight: String,
}

#[allow(dead_code)]
#[derive(Deserialize, Serialize)]
struct StateCursor {
    anchor: StatePositionData,
    head: StatePositionData,
}

#[allow(dead_code)]
#[derive(Deserialize, Serialize)]
struct StatePositionData {
    tname: String,
    item: StatePosition,
    assoc: i32,
}

#[derive(Deserialize, Serialize)]
struct StatePosition {
    client: u64,
    clock: u32,
}

#[tokio::main]
async fn main() {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("codemirror");

    let url = String::from("ws://127.0.0.1:8000/my-room/");
    let (ws_stream, _) = connect_async(&url).await.expect("Failed to connect");

    let (write, read) = ws_stream.split();

    let adapted_read = Box::pin(read.filter_map(|msg| async {
        match msg {
            Ok(TokioMessage::Binary(data)) => Some(Ok::<Vec<u8>, yrs::sync::Error>(data)),
            Ok(_) => None, // Ignore non-binary messages
            Err(e) => Some(Err(yrs::sync::Error::Other(Box::new(e)))),
        }
    }));

    let adapted_write = Box::pin(
        write
            .sink_map_err(|e| yrs::sync::Error::Other(Box::new(e)))
            .with(|data: Vec<u8>| async { Ok::<_, yrs::sync::Error>(TokioMessage::Binary(data)) }),
    );

    let arc_awareness = Arc::new(RwLock::new(Awareness::new(doc.clone())));

    let conn = Connection::new(arc_awareness, adapted_write, adapted_read);

    let sink = conn.sink();

    let mycursor = Arc::new(RwLock::new(
        text.sticky_index(&mut doc.transact_mut(), 0, Assoc::Before)
            .unwrap(),
    ));

    let (redraw_tx, mut redraw_rx) = tokio::sync::mpsc::channel(100);
    let awareness_clone = conn.awareness().clone();
    let cursor_clone = mycursor.clone();
    tokio::spawn(async move {
        while let Some(_) = redraw_rx.recv().await {
            let awareness = awareness_clone.read().await;
            let cursor = cursor_clone.read().await;
            draw(&awareness, &cursor);
        }
    });

    let redraw_tx_clone = redraw_tx.clone();
    let sink_clone = sink.clone();
    let _subscription = doc.clone().observe_update_v1(move |_txn, e| {
        let update = e.update.to_owned();
        if let Some(sink) = sink_clone.upgrade() {
            // sends the update to others clients
            // FIXME doesn't this also resend the ones received?
            task::spawn(async move {
                let msg = Message::Sync(SyncMessage::Update(update)).encode_v1();
                let mut sink = sink.lock().await;
                sink.send(msg).await.unwrap();
            });
        }

        let redraw_tx_clone = redraw_tx_clone.clone();
        task::spawn(async move {
            let _ = redraw_tx_clone.send(()).await;
        });
    });

    let redraw_tx_clone = redraw_tx.clone();
    let awareness_clone = conn.awareness().clone();
    let _subscription = tokio::spawn(async move {
        let awareness = awareness_clone.read().await;
        let redraw_tx_clone = redraw_tx_clone.clone();
        let subscription = awareness.on_update(move |_a, _e, _o| {
            let redraw_tx_clone = redraw_tx_clone.clone();
            task::spawn(async move {
                let _ = redraw_tx_clone.send(()).await;
            });
        });
        subscription
    });

    let mut stdout = stdout();
    let _ = terminal::enable_raw_mode();
    let _ = execute!(stdout, terminal::EnterAlternateScreen, cursor::Hide);

    let awareness = conn.awareness().clone();
    tokio::select! {
        _ = conn => {
            eprintln!("Connection closed");
        }
        _ = handle_user_input(awareness, mycursor, redraw_tx.clone() ) => { }
    }
    let _ = execute!(stdout, terminal::LeaveAlternateScreen, cursor::Show);
    let _ = terminal::disable_raw_mode();
}

fn draw(awareness: &Awareness, mycursor: &StickyIndex) {
    let mut stdout = stdout();
    let _ = queue!(
        stdout,
        terminal::Clear(ClearType::All),
        cursor::MoveTo(0, 0)
    );

    let doc = awareness.doc();
    let text = doc.get_or_insert_text("codemirror");

    let txn = doc.transact();
    let contents = text.get_string(&txn);

    let mut cursors: HashSet<u32> = HashSet::new();
    for (_client_id, state) in awareness.iter() {
        if let Some(data) = state.data {
            if let Ok(cursor_data) = serde_json::from_str::<State>(&data) {
                let iclient = cursor_data.cursor.anchor.item.client;
                let iclock = cursor_data.cursor.anchor.item.clock;
                let id = ID::new(iclient, iclock);
                let sticky = StickyIndex::from_id(id, Assoc::After);
                if let Some(offset) = sticky.get_offset(&txn) {
                    cursors.insert(offset.index);
                }
            }
        }
    }

    let mycursor_offset = match mycursor.get_offset(&txn) {
        Some(o) => Some(o.index),
        None => None,
    };

    let (term_cols, _) = terminal::size().unwrap();
    let _ = queue!(
        stdout,
        SetAttribute(Attribute::Reverse),
        Print(" ".repeat(term_cols as usize)),
        cursor::MoveTo(0, 0),
        Print("  example code-mirror-client                       press ESC to exit"),
        SetAttribute(Attribute::Reset)
    );

    let mut linenr = 0;
    let _ = queue!(stdout, cursor::MoveTo(0, 1));
    for (idx, ch) in contents.chars().enumerate() {
        match ch {
            '\n' => {
                linenr += 1;
                let _ = queue!(stdout, cursor::MoveTo(0, linenr + 1));
            }
            _ => {
                if Some(idx as u32) == mycursor_offset {
                    // Highlight the local cursor
                    let _ = queue!(
                        stdout,
                        SetBackgroundColor(Color::Red),
                        SetForegroundColor(Color::White),
                        Print(ch),
                        ResetColor
                    );
                } else if cursors.contains(&(idx as u32)) {
                    let _ = queue!(
                        stdout,
                        SetBackgroundColor(Color::Blue),
                        SetForegroundColor(Color::White),
                        Print(ch),
                        ResetColor
                    );
                } else {
                    let _ = queue!(stdout, Print(ch));
                }
            }
        }
    }

    let _ = stdout.flush();
}

async fn handle_user_input(
    awareness: Arc<RwLock<Awareness>>,
    cursor: Arc<RwLock<StickyIndex>>,
    redraw_tx: tokio::sync::mpsc::Sender<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        if let Event::Key(key) = event::read()? {
            let awareness = awareness.write().await;
            let doc = awareness.doc();
            let text = doc.get_or_insert_text("codemirror");

            match key.code {
                KeyCode::Backspace => {
                    // Delete the last character
                    let mut txn = doc.transact_mut();
                    let mut cursor_lock = cursor.write().await;
                    if let Some(offset) = cursor_lock.get_offset(&txn) {
                        if (offset.index > 0) {
                            text.remove_range(&mut txn, offset.index - 1, 1);
                        }
                        //    *cursor_lock = text
                        //        .sticky_index(&mut txn, offset.index + 1, Assoc::Before)
                        //        .unwrap();
                    }
                }

                KeyCode::Char(c) => {
                    let mut txn = doc.transact_mut();
                    let mut cursor_lock = cursor.write().await;
                    if let Some(offset) = cursor_lock.get_offset(&txn) {
                        text.insert(&mut txn, offset.index, &c.to_string());
                        *cursor_lock = text
                            .sticky_index(&mut txn, offset.index + 1, Assoc::Before)
                            .unwrap();
                    }
                }

                KeyCode::Esc => {
                    break;
                }

                KeyCode::Left => {
                    let mut txn = doc.transact_mut();
                    let mut cursor_lock = cursor.write().await;
                    if let Some(offset) = cursor_lock.get_offset(&txn) {
                        if offset.index > 0 {
                            *cursor_lock = text
                                .sticky_index(&mut txn, offset.index - 1, Assoc::Before)
                                .unwrap();
                        }
                    }
                    let _ = redraw_tx.send(()).await;
                }

                KeyCode::Right => {
                    let mut txn = doc.transact_mut();
                    let mut cursor_lock = cursor.write().await;
                    if let Some(offset) = cursor_lock.get_offset(&txn) {
                        if offset.index < text.len(&txn) {
                            *cursor_lock = text
                                .sticky_index(&mut txn, offset.index + 1, Assoc::Before)
                                .unwrap();
                        }
                    }
                    let _ = redraw_tx.send(()).await;
                }

                _ => {}
            }
        }
    }

    Ok(())
}
