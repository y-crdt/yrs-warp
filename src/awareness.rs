use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::broadcast::Sender;
use yrs::block::ClientID;
use yrs::updates::decoder::{Decode, Decoder};
use yrs::updates::encoder::{Encode, Encoder};
use yrs::Doc;

pub const OUTDATED_TIMEOUT: Duration = Duration::from_millis(3000);

const NULL_STR: &str = "null";

/**
 * The Awareness class implements a simple shared state protocol that can be used for non-persistent data like awareness information
 * (cursor, username, status, ..). Each client can update its own local state and listen to state changes of
 * remote clients. Every client may set a state of a remote peer to `null` to mark the client as offline.
 *
 * Each client is identified by a unique client id (something we borrow from `doc.clientID`). A client can override
 * its own state by propagating a message with an increasing timestamp (`clock`). If such a message is received, it is
 * applied if the known state of that client is older than the new state (`clock < newClock`). If a client thinks that
 * a remote client is offline, it may propagate a message with
 * `{ clock: currentClientClock, state: null, client: remoteClient }`. If such a
 * message is received, and the known clock of that client equals the received clock, it will override the state with `null`.
 *
 * Before a client disconnects, it should propagate a `null` state with an updated clock.
 *
 * Awareness states must be updated every 30 seconds. Otherwise the Awareness instance will delete the client state.
 *
 * @extends {Observable<string>}
 */
pub struct Awareness {
    doc: Doc,
    states: HashMap<ClientID, String>,
    meta: HashMap<ClientID, MetaClientState>,
    on_update: Option<Sender<Event>>,
}

unsafe impl Send for Awareness {}
unsafe impl Sync for Awareness {}

impl Awareness {
    pub fn new(doc: Doc) -> Self {
        Awareness {
            doc,
            on_update: None,
            states: HashMap::new(),
            meta: HashMap::new(),
        }
    }

    pub fn with_observer(doc: Doc, on_update: Sender<Event>) -> Self {
        Awareness {
            doc,
            on_update: Some(on_update),
            states: HashMap::new(),
            meta: HashMap::new(),
        }
    }

    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    pub fn client_id(&self) -> ClientID {
        self.doc.client_id
    }

    pub fn states(&self) -> &HashMap<ClientID, String> {
        &self.states
    }

    pub fn local_state(&self) -> Option<&str> {
        Some(self.states.get(&self.doc.client_id)?.as_str())
    }

    pub fn set_local_state<S: Into<String>>(&mut self, state: S) {
        let client_id = self.doc.client_id;
        self.update_meta(client_id);
        let new: String = state.into();
        match self.states.entry(client_id) {
            Entry::Occupied(mut e) => {
                e.insert(new);
                if let Some(sender) = self.on_update.as_mut() {
                    let _ = sender.send(Event::new(vec![], vec![client_id], vec![]));
                }
            }
            Entry::Vacant(e) => {
                e.insert(new);
                if let Some(sender) = self.on_update.as_mut() {
                    let _ = sender.send(Event::new(vec![client_id], vec![], vec![]));
                }
            }
        };
    }

    pub fn remove_state(&mut self, client_id: ClientID) {
        let prev_state = self.states.remove(&client_id);
        self.update_meta(client_id);
        if let Some(sender) = self.on_update.as_mut() {
            if prev_state.is_some() {
                let _ = sender.send(Event::new(Vec::default(), Vec::default(), vec![client_id]));
            }
        }
    }

    pub fn clean_local_state(&mut self) {
        let client_id = self.doc.client_id;
        self.remove_state(client_id);
    }

    fn update_meta(&mut self, client_id: ClientID) {
        match self.meta.entry(client_id) {
            Entry::Occupied(mut e) => {
                let clock = e.get().clock + 1;
                let meta = MetaClientState::new(clock, Instant::now());
                e.insert(meta);
            }
            Entry::Vacant(e) => {
                e.insert(MetaClientState::new(1, Instant::now()));
            }
        }
    }

    pub fn update(&self) -> Result<AwarenessUpdate, Error> {
        let clients = self.states.keys().cloned();
        self.update_with_clients(clients)
    }

    pub fn update_with_clients<I: IntoIterator<Item = ClientID>>(
        &self,
        clients: I,
    ) -> Result<AwarenessUpdate, Error> {
        let mut res = HashMap::new();
        for client_id in clients {
            let clock = if let Some(meta) = self.meta.get(&client_id) {
                meta.clock
            } else {
                return Err(Error::ClientNotFound(client_id));
            };
            let json = if let Some(json) = self.states.get(&client_id) {
                json.clone()
            } else {
                String::from(NULL_STR)
            };
            res.insert(client_id, AwarenessUpdateEntry { clock, json });
        }
        Ok(AwarenessUpdate { clients: res })
    }

    pub fn apply_update(&mut self, update: AwarenessUpdate) -> Result<(), Error> {
        let now = Instant::now();

        let mut added = Vec::new();
        let mut updated = Vec::new();
        let mut removed = Vec::new();

        for (client_id, entry) in update.clients {
            let mut clock = entry.clock;
            let is_null = entry.json.as_str() == NULL_STR;
            match self.meta.entry(client_id) {
                Entry::Occupied(mut e) => {
                    let prev = e.get();
                    let is_removed =
                        prev.clock == clock && is_null && self.states.contains_key(&client_id);
                    let is_new = prev.clock < clock;
                    if is_new || is_removed {
                        if is_null {
                            // never let a remote client remove this local state
                            if client_id == self.doc.client_id
                                && self.states.get(&client_id).is_some()
                            {
                                // remote client removed the local state. Do not remote state. Broadcast a message indicating
                                // that this client still exists by increasing the clock
                                clock += 1;
                            } else {
                                self.states.remove(&client_id);
                                if self.on_update.is_some() {
                                    removed.push(client_id);
                                }
                            }
                        } else {
                            match self.states.entry(client_id) {
                                Entry::Occupied(mut e) => {
                                    if self.on_update.is_some() {
                                        updated.push(client_id);
                                    }
                                    e.insert(entry.json);
                                }
                                Entry::Vacant(e) => {
                                    e.insert(entry.json);
                                    if self.on_update.is_some() {
                                        updated.push(client_id);
                                    }
                                }
                            }
                        }
                        e.insert(MetaClientState::new(clock, now));
                        true
                    } else {
                        false
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(MetaClientState::new(clock, now));
                    self.states.insert(client_id, entry.json);
                    if self.on_update.is_some() {
                        added.push(client_id);
                    }
                    true
                }
            };
        }

        if let Some(sender) = self.on_update.as_mut() {
            if !added.is_empty() || !updated.is_empty() || !removed.is_empty() {
                let _ = sender.send(Event::new(added, updated, removed));
            }
        }

        Ok(())
    }
}

impl Default for Awareness {
    fn default() -> Self {
        Awareness::new(Doc::new())
    }
}

#[derive(Debug)]
pub struct AwarenessUpdate {
    clients: HashMap<ClientID, AwarenessUpdateEntry>,
}

impl AwarenessUpdate {
    pub fn new() -> Self {
        AwarenessUpdate {
            clients: HashMap::new(),
        }
    }
}

impl Encode for AwarenessUpdate {
    fn encode<E: Encoder>(&self, encoder: &mut E) {
        encoder.write_var(self.clients.len());
        for (&client_id, e) in self.clients.iter() {
            encoder.write_var(client_id);
            encoder.write_var(e.clock);
            encoder.write_string(&e.json);
        }
    }
}

impl Decode for AwarenessUpdate {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, lib0::error::Error> {
        let len: usize = decoder.read_var()?;
        let mut clients = HashMap::with_capacity(len);
        for _ in 0..len {
            let client_id: ClientID = decoder.read_var()?;
            let clock: u32 = decoder.read_var()?;
            let json = decoder.read_string()?.to_string();
            clients.insert(client_id, AwarenessUpdateEntry { clock, json });
        }

        Ok(AwarenessUpdate { clients })
    }
}

#[derive(Debug)]
pub struct AwarenessUpdateEntry {
    clock: u32,
    json: String,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("client ID `{0}` not found")]
    ClientNotFound(ClientID),
    #[error("failed to deserialize awareness update")]
    DeserializationError(#[from] lib0::error::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaClientState {
    clock: u32,
    last_updated: Instant,
}

impl MetaClientState {
    fn new(clock: u32, last_updated: Instant) -> Self {
        MetaClientState {
            clock,
            last_updated,
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Event {
    added: Vec<ClientID>,
    updated: Vec<ClientID>,
    removed: Vec<ClientID>,
}

impl Event {
    pub fn new(added: Vec<ClientID>, updated: Vec<ClientID>, removed: Vec<ClientID>) -> Self {
        Event {
            added,
            updated,
            removed,
        }
    }

    pub fn added(&self) -> &[ClientID] {
        &self.added
    }

    pub fn updated(&self) -> &[ClientID] {
        &self.updated
    }

    pub fn removed(&self) -> &[ClientID] {
        &self.removed
    }

    fn clear(&mut self) {
        self.added.clear();
        self.updated.clear();
        self.removed.clear();
    }
}

#[cfg(test)]
mod test {
    use crate::awareness::{Awareness, Event};
    use tokio::sync::broadcast::{channel, Receiver};
    use yrs::Doc;

    fn update(
        recv: &mut Receiver<Event>,
        from: &Awareness,
        to: &mut Awareness,
    ) -> Result<Event, Box<dyn std::error::Error>> {
        let e = recv.try_recv()?;
        let u = from.update_with_clients([e.added(), e.updated(), e.removed()].concat())?;
        to.apply_update(u)?;
        Ok(e)
    }

    #[test]
    fn awareness() -> Result<(), Box<dyn std::error::Error>> {
        let (s1, mut o_local) = channel(1);
        let mut local = Awareness::with_observer(Doc::with_client_id(1), s1);

        let (s2, mut o_remote) = channel(1);
        let mut remote = Awareness::with_observer(Doc::with_client_id(2), s2);

        local.set_local_state("{x:3}");
        let mut e_local = update(&mut o_local, &local, &mut remote)?;
        assert_eq!(remote.states()[&1], "{x:3}");
        assert_eq!(remote.meta[&1].clock, 1);
        assert_eq!(o_remote.try_recv()?.added, &[1]);

        local.set_local_state("{x:4}");
        e_local = update(&mut o_local, &local, &mut remote)?;
        let e_remote = o_remote.try_recv()?;
        assert_eq!(remote.states()[&1], "{x:4}");
        assert_eq!(e_remote, Event::new(vec![], vec![1], vec![]));
        assert_eq!(e_remote, e_local);

        local.clean_local_state();
        e_local = update(&mut o_local, &local, &mut remote)?;
        let e_remote = o_remote.try_recv()?;
        assert_eq!(e_remote.removed.len(), 1);
        assert_eq!(local.states().get(&1), None);
        assert_eq!(e_remote, e_local);
        Ok(())
    }
}
