//! In-memory asynchronous operation resources and idempotency keys.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::Principal;
use crate::error::ServerError;
use crate::events::EventHub;

const RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationErrorDto {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub outcome: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: Uuid,
    pub kind: String,
    pub state: OperationState,
    pub identity: String,
    pub role: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub result: Option<Value>,
    pub error: Option<OperationErrorDto>,
    pub outcome: String,
}

#[derive(Clone)]
pub struct OperationStore {
    inner: Arc<Mutex<StoreInner>>,
    events: EventHub,
}

struct StoreInner {
    records: HashMap<Uuid, OperationRecord>,
    idempotency: HashMap<String, IdempotencyRecord>,
}

struct IdempotencyRecord {
    fingerprint: String,
    operation_id: Uuid,
}

pub enum CreateOperation {
    New(OperationRecord),
    Existing(OperationRecord),
}

impl OperationStore {
    pub fn new(events: EventHub) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                records: HashMap::new(),
                idempotency: HashMap::new(),
            })),
            events,
        }
    }

    pub fn create(
        &self,
        kind: impl Into<String>,
        principal: &Principal,
        idempotency_key: &str,
        fingerprint: String,
    ) -> Result<CreateOperation, ServerError> {
        let key = idempotency_key.trim();
        if key.is_empty() || key.len() > 255 {
            return Err(ServerError::bad_request(
                "Idempotency-Key must contain 1 to 255 characters",
            ));
        }
        let kind = kind.into();
        let fingerprint = format!("{kind}:{fingerprint}");
        let scoped_key = format!("{}:{key}", principal.name);
        let mut inner = self.inner.lock().expect("operation store poisoned");
        cleanup(&mut inner);
        if let Some(existing) = inner.idempotency.get(&scoped_key) {
            if existing.fingerprint != fingerprint {
                return Err(ServerError::conflict(
                    "Idempotency-Key was already used with different input",
                ));
            }
            let record = inner
                .records
                .get(&existing.operation_id)
                .expect("idempotency record without operation")
                .clone();
            return Ok(CreateOperation::Existing(record));
        }

        let id = Uuid::new_v4();
        let record = OperationRecord {
            id,
            kind,
            state: OperationState::Queued,
            identity: principal.name.clone(),
            role: format!("{:?}", principal.role).to_ascii_lowercase(),
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            result: None,
            error: None,
            outcome: "not_started".to_string(),
        };
        inner.records.insert(id, record.clone());
        inner.idempotency.insert(
            scoped_key,
            IdempotencyRecord {
                fingerprint,
                operation_id: id,
            },
        );
        drop(inner);
        self.publish(&record);
        Ok(CreateOperation::New(record))
    }

    pub fn get(&self, id: Uuid) -> Option<OperationRecord> {
        let mut inner = self.inner.lock().expect("operation store poisoned");
        cleanup(&mut inner);
        inner.records.get(&id).cloned()
    }

    pub fn running(&self, id: Uuid) {
        self.update(id, |record| {
            record.state = OperationState::Running;
            record.started_at = Some(Utc::now());
            record.outcome = "unknown".to_string();
        });
    }

    pub fn succeeded(&self, id: Uuid, result: Value) {
        self.update(id, |record| {
            record.state = OperationState::Succeeded;
            record.finished_at = Some(Utc::now());
            record.result = Some(result);
            record.error = None;
            record.outcome = "succeeded".to_string();
        });
    }

    pub fn failed(&self, id: Uuid, error: ServerError) {
        self.update(id, |record| {
            record.state = if error.outcome == "unknown" {
                OperationState::Unknown
            } else {
                OperationState::Failed
            };
            record.finished_at = Some(Utc::now());
            record.outcome = error.outcome.to_string();
            record.error = Some(OperationErrorDto {
                code: error.code.to_string(),
                message: error.message,
                retryable: error.retryable,
                outcome: error.outcome.to_string(),
            });
        });
    }

    fn update(&self, id: Uuid, change: impl FnOnce(&mut OperationRecord)) {
        let record = {
            let mut inner = self.inner.lock().expect("operation store poisoned");
            let Some(record) = inner.records.get_mut(&id) else {
                return;
            };
            change(record);
            record.clone()
        };
        self.publish(&record);
    }

    fn publish(&self, record: &OperationRecord) {
        self.events.publish(
            "operation",
            json!({
                "id": record.id,
                "kind": record.kind,
                "state": record.state,
                "outcome": record.outcome,
            }),
        );
    }
}

fn cleanup(inner: &mut StoreInner) {
    let cutoff = Utc::now()
        - chrono::Duration::from_std(RETENTION).expect("operation retention fits chrono duration");
    inner
        .records
        .retain(|_, record| record.created_at >= cutoff);
    inner
        .idempotency
        .retain(|_, record| inner.records.contains_key(&record.operation_id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Role;

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.to_string(),
            role: Role::Controller,
        }
    }

    #[test]
    fn idempotency_reuses_identical_input_and_rejects_mismatch() {
        let store = OperationStore::new(EventHub::new());
        let owner = principal("controller");
        let CreateOperation::New(first) = store
            .create("run_start", &owner, "key", "same".to_string())
            .unwrap()
        else {
            panic!("first operation was not new");
        };
        let CreateOperation::Existing(second) = store
            .create("run_start", &owner, "key", "same".to_string())
            .unwrap()
        else {
            panic!("identical retry did not reuse operation");
        };
        assert_eq!(first.id, second.id);
        assert!(store
            .create("run_start", &owner, "key", "different".to_string())
            .is_err());
        assert!(store
            .create("run_stop", &owner, "key", "same".to_string())
            .is_err());
    }

    #[test]
    fn idempotency_keys_are_scoped_to_identity() {
        let store = OperationStore::new(EventHub::new());
        let CreateOperation::New(first) = store
            .create("run_start", &principal("one"), "key", "body".to_string())
            .unwrap()
        else {
            panic!();
        };
        let CreateOperation::New(second) = store
            .create("run_start", &principal("two"), "key", "body".to_string())
            .unwrap()
        else {
            panic!();
        };
        assert_ne!(first.id, second.id);
    }
}
