use std::sync::Arc;
use tokio::sync::RwLock;
use y_sync::awareness::Awareness;

pub mod signaling;
pub mod ws;

pub type BroadcastGroup = y_sync::net::BroadcastGroup;
pub type AwarenessRef = Arc<RwLock<Awareness>>;
