use crate::ws::Inbox;
use crate::AwarenessRef;
use lib0::encoding::Write;
use tokio::spawn;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use y_sync::awareness;
use y_sync::awareness::Event;
use y_sync::sync::{Message, MSG_SYNC, MSG_SYNC_UPDATE};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::UpdateSubscription;

/// A broadcast group can be used to propagate updates produced by yrs [yrs::Doc] and [Awareness]
/// structures in a binary form that conforms to a y-sync protocol.
///
/// New receivers can subscribe to a broadcasting group via [BroadcastGroup::join] method.
pub struct BroadcastGroup {
    awareness_sub: awareness::Subscription<Event>,
    doc_sub: UpdateSubscription,
    awareness_ref: AwarenessRef,
    sender: Sender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
}

unsafe impl Send for BroadcastGroup {}
unsafe impl Sync for BroadcastGroup {}

impl BroadcastGroup {
    pub async fn open(awareness_ref: AwarenessRef, capacity: usize) -> Self {
        let (sender, receiver) = channel(capacity);
        let (doc_sub, awareness_sub) = {
            let mut awareness = awareness_ref.write().await;
            let sink = sender.clone();
            let doc_sub = awareness
                .doc_mut()
                .observe_update_v1(move |_txn, u| {
                    // we manually construct msg here to avoid update data copying
                    let mut encoder = EncoderV1::new();
                    encoder.write_var(MSG_SYNC);
                    encoder.write_var(MSG_SYNC_UPDATE);
                    encoder.write_buf(&u.update);
                    let msg = encoder.to_vec();
                    if let Err(e) = sink.send(msg) {
                        panic!("couldn't broadcast the document update: {}", e);
                    }
                })
                .unwrap();
            let sink = sender.clone();
            let awareness_sub = awareness.on_update(move |awareness, e| {
                let added = e.added();
                let updated = e.updated();
                let removed = e.removed();
                let mut changed = Vec::with_capacity(added.len() + updated.len() + removed.len());
                changed.extend_from_slice(added);
                changed.extend_from_slice(updated);
                changed.extend_from_slice(removed);

                if let Ok(u) = awareness.update_with_clients(changed) {
                    let msg = Message::Awareness(u).encode_v1();
                    if let Err(e) = sink.send(msg) {
                        panic!("couldn't broadcast awareness update: {}", e)
                    }
                }
            });
            (doc_sub, awareness_sub)
        };
        BroadcastGroup {
            awareness_ref,
            sender,
            receiver,
            awareness_sub,
            doc_sub,
        }
    }

    /// Subscribes a new BroadcastGroup gossip receiver. Returned join handle serves as a
    /// subscription handler - dropping it will unsubscribe receiver from the group. It can also
    /// finish abruptly if subscriber has been closed or couldn't propagate gossips for any reason.
    pub fn join<I>(&self, mut subscriber: I) -> JoinHandle<Result<(), I::Error>>
    where
        I: Inbox<Item = Vec<u8>> + Send + Sync + 'static,
    {
        let mut receiver = self.sender.subscribe();
        spawn(async move {
            while let Ok(msg) = receiver.recv().await {
                if let Err(e) = subscriber.send(msg).await {
                    return Err(e);
                }
            }
            Ok(())
        })
    }
}
