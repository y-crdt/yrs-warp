use std::sync::Arc;
use tokio::sync::RwLock;
use y_sync::awareness::Awareness;

pub mod broadcast;
pub mod ws;

pub type AwarenessRef = Arc<RwLock<Awareness>>;