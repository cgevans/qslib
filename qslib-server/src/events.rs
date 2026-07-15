//! Bounded process-local event history used by SSE clients.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;

pub const EVENT_HISTORY: usize = 4096;
pub const SUBSCRIBER_BUFFER: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: u64,
    pub event: String,
    pub timestamp: DateTime<Utc>,
    pub data: Value,
}

#[derive(Clone)]
pub struct EventHub {
    next_id: Arc<AtomicU64>,
    history: Arc<Mutex<VecDeque<EventEnvelope>>>,
    sender: broadcast::Sender<EventEnvelope>,
}

pub enum Replay {
    Events(Vec<EventEnvelope>),
    Expired,
}

impl EventHub {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(SUBSCRIBER_BUFFER);
        Self {
            next_id: Arc::new(AtomicU64::new(1)),
            history: Arc::new(Mutex::new(VecDeque::with_capacity(EVENT_HISTORY))),
            sender,
        }
    }

    pub fn publish(&self, event: impl Into<String>, data: Value) -> EventEnvelope {
        let envelope = EventEnvelope {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            event: event.into(),
            timestamp: Utc::now(),
            data,
        };
        {
            let mut history = self.history.lock().expect("event history poisoned");
            if history.len() == EVENT_HISTORY {
                history.pop_front();
            }
            history.push_back(envelope.clone());
        }
        let _ = self.sender.send(envelope.clone());
        envelope
    }

    pub fn replay_after(&self, last_id: Option<u64>) -> Replay {
        let Some(last_id) = last_id else {
            return Replay::Events(Vec::new());
        };
        let history = self.history.lock().expect("event history poisoned");
        if let Some(oldest) = history.front() {
            if last_id.saturating_add(1) < oldest.id {
                return Replay::Expired;
            }
        }
        Replay::Events(
            history
                .iter()
                .filter(|event| event.id > last_id)
                .cloned()
                .collect(),
        )
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.sender.subscribe()
    }
}

impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ids_are_monotonic_and_replay_is_ordered() {
        let hub = EventHub::new();
        let first = hub.publish("one", json!({"n": 1}));
        let second = hub.publish("two", json!({"n": 2}));
        assert!(second.id > first.id);
        let Replay::Events(replayed) = hub.replay_after(Some(first.id)) else {
            panic!("history unexpectedly expired");
        };
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].id, second.id);
    }

    #[test]
    fn history_expiration_is_reported() {
        let hub = EventHub::new();
        for n in 0..=EVENT_HISTORY {
            hub.publish("event", json!({"n": n}));
        }
        assert!(matches!(hub.replay_after(Some(0)), Replay::Expired));
    }
}
