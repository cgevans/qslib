//! Durable ingestion cursor and delivery queue.
//!
//! All SQLite access is serialized through one actor. Ingestion commits the
//! raw SSE event, derived sink work, and the machine cursor in one transaction.

use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};

const DATABASE_CHANNEL_CAPACITY: usize = 256;
const SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Clone)]
pub struct MatrixOutput {
    pub room: String,
    pub body: String,
    pub transaction_id: String,
}

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub machine: String,
    pub cursor: Option<String>,
    pub raw_json: String,
    pub received_at: i64,
    pub influx_lines: Vec<String>,
    pub matrix: Option<MatrixOutput>,
    pub processing_error: Option<String>,
    pub dead_letter: bool,
}

#[derive(Debug, Clone)]
pub struct InfluxBatch {
    pub ids: Vec<i64>,
    pub body: String,
}

#[derive(Debug, Clone)]
pub struct MatrixDelivery {
    pub id: i64,
    pub room: String,
    pub body: String,
    pub transaction_id: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueStats {
    pub pending: u64,
    pub oldest_pending_seconds: Option<u64>,
    pub retries: u64,
    pub dead_letters: u64,
}

#[derive(Clone)]
pub struct Database {
    tx: mpsc::Sender<Command>,
}

pub struct DatabaseActor {
    connection: Connection,
    rx: mpsc::Receiver<Command>,
    // Holding this descriptor keeps the process-wide exclusive lock alive.
    _lock: File,
}

enum Command {
    Cursor {
        machine: String,
        reply: oneshot::Sender<Result<Option<String>>>,
    },
    Commit {
        record: EventRecord,
        reply: oneshot::Sender<Result<()>>,
    },
    InfluxBatch {
        limit: usize,
        now: i64,
        reply: oneshot::Sender<Result<Option<InfluxBatch>>>,
    },
    InfluxDelivered {
        ids: Vec<i64>,
        now: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    InfluxFailed {
        ids: Vec<i64>,
        error: String,
        retry_at: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    MatrixDelivery {
        now: i64,
        reply: oneshot::Sender<Result<Option<MatrixDelivery>>>,
    },
    MatrixDelivered {
        id: i64,
        now: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    MatrixFailed {
        id: i64,
        error: String,
        retry_at: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    Stats {
        now: i64,
        reply: oneshot::Sender<Result<QueueStats>>,
    },
    Prune {
        before: i64,
        reply: oneshot::Sender<Result<u64>>,
    },
    Ping {
        reply: oneshot::Sender<()>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<()>>,
    },
}

impl DatabaseActor {
    pub fn open(path: &Path) -> Result<(Database, Self)> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state directory {parent:?}"))?;
        }

        let lock_path = lock_path(path);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("opening database lock {lock_path:?}"))?;
        let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            return Err(anyhow!(
                "another qs-monitor owns the state database lock {:?}: {}",
                lock_path,
                std::io::Error::last_os_error()
            ));
        }

        let mut connection =
            Connection::open(path).with_context(|| format!("opening state database {path:?}"))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        migrate(&mut connection)?;

        let (tx, rx) = mpsc::channel(DATABASE_CHANNEL_CAPACITY);
        Ok((
            Database { tx },
            Self {
                connection,
                rx,
                _lock: lock,
            },
        ))
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(command) = self.rx.recv().await {
            match command {
                Command::Cursor { machine, reply } => {
                    let _ = reply.send(cursor(&self.connection, &machine));
                }
                Command::Commit { record, reply } => {
                    let _ = reply.send(commit(&mut self.connection, record));
                }
                Command::InfluxBatch { limit, now, reply } => {
                    let _ = reply.send(influx_batch(&self.connection, limit, now));
                }
                Command::InfluxDelivered { ids, now, reply } => {
                    let _ = reply.send(mark_influx_delivered(&mut self.connection, &ids, now));
                }
                Command::InfluxFailed {
                    ids,
                    error,
                    retry_at,
                    reply,
                } => {
                    let _ = reply.send(mark_influx_failed(
                        &mut self.connection,
                        &ids,
                        &error,
                        retry_at,
                    ));
                }
                Command::MatrixDelivery { now, reply } => {
                    let _ = reply.send(matrix_delivery(&self.connection, now));
                }
                Command::MatrixDelivered { id, now, reply } => {
                    let _ = reply.send(mark_matrix_delivered(&mut self.connection, id, now));
                }
                Command::MatrixFailed {
                    id,
                    error,
                    retry_at,
                    reply,
                } => {
                    let _ = reply.send(mark_matrix_failed(&self.connection, id, &error, retry_at));
                }
                Command::Stats { now, reply } => {
                    let _ = reply.send(stats(&self.connection, now));
                }
                Command::Prune { before, reply } => {
                    let _ = reply.send(prune(&self.connection, before));
                }
                Command::Ping { reply } => {
                    let _ = reply.send(());
                }
                Command::Shutdown { reply } => {
                    let result = self
                        .connection
                        .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")
                        .map_err(Into::into);
                    let _ = reply.send(result);
                    return Ok(());
                }
            }
        }
        Err(anyhow!("database command channel closed unexpectedly"))
    }
}

impl Database {
    pub async fn cursor(&self, machine: &str) -> Result<Option<String>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Cursor {
                machine: machine.to_string(),
                reply,
            })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn commit(&self, record: EventRecord) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Commit { record, reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn influx_batch(&self, limit: usize, now: i64) -> Result<Option<InfluxBatch>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InfluxBatch { limit, now, reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn mark_influx_delivered(&self, ids: Vec<i64>, now: i64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InfluxDelivered { ids, now, reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn mark_influx_failed(
        &self,
        ids: Vec<i64>,
        error: String,
        retry_at: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InfluxFailed {
                ids,
                error,
                retry_at,
                reply,
            })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn matrix_delivery(&self, now: i64) -> Result<Option<MatrixDelivery>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::MatrixDelivery { now, reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn mark_matrix_delivered(&self, id: i64, now: i64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::MatrixDelivered { id, now, reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn mark_matrix_failed(&self, id: i64, error: String, retry_at: i64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::MatrixFailed {
                id,
                error,
                retry_at,
                reply,
            })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn stats(&self, now: i64) -> Result<QueueStats> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Stats { now, reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn prune(&self, before: i64) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Prune { before, reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }

    pub async fn ping(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Ping { reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::Shutdown { reply })
            .await
            .map_err(|_| anyhow!("database actor stopped"))?;
        rx.await.map_err(|_| anyhow!("database actor stopped"))?
    }
}

fn lock_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".lock");
    value.into()
}

fn migrate(connection: &mut Connection) -> Result<()> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS machine_cursors (
            machine TEXT PRIMARY KEY NOT NULL,
            cursor TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine TEXT NOT NULL,
            cursor TEXT,
            raw_event_json TEXT NOT NULL,
            received_at INTEGER NOT NULL,
            influx_lines TEXT NOT NULL DEFAULT '[]',
            matrix_room TEXT,
            matrix_body TEXT,
            matrix_transaction_id TEXT,
            influx_delivered_at INTEGER,
            matrix_delivered_at INTEGER,
            influx_attempts INTEGER NOT NULL DEFAULT 0,
            matrix_attempts INTEGER NOT NULL DEFAULT 0,
            influx_next_attempt_at INTEGER NOT NULL DEFAULT 0,
            matrix_next_attempt_at INTEGER NOT NULL DEFAULT 0,
            last_influx_error TEXT,
            last_matrix_error TEXT,
            processing_error TEXT,
            dead_letter INTEGER NOT NULL DEFAULT 0,
            fully_delivered_at INTEGER,
            UNIQUE(machine, cursor)
        );
        CREATE INDEX IF NOT EXISTS events_influx_pending
            ON events(influx_delivered_at, influx_next_attempt_at, id);
        CREATE INDEX IF NOT EXISTS events_matrix_pending
            ON events(matrix_delivered_at, matrix_next_attempt_at, id);
        CREATE INDEX IF NOT EXISTS events_fully_delivered
            ON events(fully_delivered_at);",
    )?;
    if version < SCHEMA_VERSION {
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    }
    transaction.commit()?;
    Ok(())
}

fn cursor(connection: &Connection, machine: &str) -> Result<Option<String>> {
    Ok(connection
        .query_row(
            "SELECT cursor FROM machine_cursors WHERE machine = ?1",
            [machine],
            |row| row.get(0),
        )
        .optional()?)
}

fn commit(connection: &mut Connection, record: EventRecord) -> Result<()> {
    let transaction = connection.transaction()?;
    let influx_json = serde_json::to_string(&record.influx_lines)?;
    let (room, body, transaction_id) = record
        .matrix
        .map(|value| {
            (
                Some(value.room),
                Some(value.body),
                Some(value.transaction_id),
            )
        })
        .unwrap_or_default();
    let complete = !record.dead_letter && record.influx_lines.is_empty() && room.is_none();
    let inserted = transaction.execute(
        "INSERT INTO events (
            machine, cursor, raw_event_json, received_at, influx_lines,
            matrix_room, matrix_body, matrix_transaction_id, processing_error,
            dead_letter, fully_delivered_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(machine, cursor) DO NOTHING",
        params![
            record.machine,
            record.cursor,
            record.raw_json,
            record.received_at,
            influx_json,
            room,
            body,
            transaction_id,
            record.processing_error,
            record.dead_letter,
            complete.then_some(record.received_at),
        ],
    )?;
    if inserted > 0
        && let Some(cursor) = record.cursor
    {
        transaction.execute(
            "INSERT INTO machine_cursors(machine, cursor, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(machine) DO UPDATE SET cursor=excluded.cursor, updated_at=excluded.updated_at",
            params![record.machine, cursor, record.received_at],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn influx_batch(connection: &Connection, limit: usize, now: i64) -> Result<Option<InfluxBatch>> {
    let mut statement = connection.prepare(
        "SELECT id, influx_lines FROM events
         WHERE dead_letter = 0
           AND influx_delivered_at IS NULL
           AND influx_lines != '[]'
           AND influx_next_attempt_at <= ?1
         ORDER BY id",
    )?;
    let mut rows = statement.query([now])?;
    let mut ids = Vec::new();
    let mut lines = Vec::new();
    while let Some(row) = rows.next()? {
        let encoded: String = row.get(1)?;
        let event_lines = serde_json::from_str::<Vec<String>>(&encoded)?;
        if !ids.is_empty() && lines.len() + event_lines.len() > limit.max(1) {
            break;
        }
        ids.push(row.get(0)?);
        lines.extend(event_lines);
        if lines.len() >= limit.max(1) {
            break;
        }
    }
    if ids.is_empty() {
        Ok(None)
    } else {
        Ok(Some(InfluxBatch {
            ids,
            body: lines.join("\n"),
        }))
    }
}

fn mark_influx_delivered(connection: &mut Connection, ids: &[i64], now: i64) -> Result<()> {
    let transaction = connection.transaction()?;
    for id in ids {
        transaction.execute(
            "UPDATE events SET influx_delivered_at=?2, last_influx_error=NULL WHERE id=?1",
            params![id, now],
        )?;
        mark_fully_delivered(&transaction, *id, now)?;
    }
    transaction.commit()?;
    Ok(())
}

fn mark_influx_failed(
    connection: &mut Connection,
    ids: &[i64],
    error: &str,
    retry_at: i64,
) -> Result<()> {
    let transaction = connection.transaction()?;
    for id in ids {
        transaction.execute(
            "UPDATE events SET influx_attempts=influx_attempts+1,
             influx_next_attempt_at=?2, last_influx_error=?3 WHERE id=?1",
            params![id, retry_at, error],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn matrix_delivery(connection: &Connection, now: i64) -> Result<Option<MatrixDelivery>> {
    Ok(connection
        .query_row(
            "SELECT id, matrix_room, matrix_body, matrix_transaction_id FROM events
             WHERE dead_letter = 0 AND matrix_room IS NOT NULL
               AND matrix_delivered_at IS NULL AND matrix_next_attempt_at <= ?1
             ORDER BY id LIMIT 1",
            [now],
            |row| {
                Ok(MatrixDelivery {
                    id: row.get(0)?,
                    room: row.get(1)?,
                    body: row.get(2)?,
                    transaction_id: row.get(3)?,
                })
            },
        )
        .optional()?)
}

fn mark_matrix_delivered(connection: &mut Connection, id: i64, now: i64) -> Result<()> {
    let transaction = connection.transaction()?;
    transaction.execute(
        "UPDATE events SET matrix_delivered_at=?2, last_matrix_error=NULL WHERE id=?1",
        params![id, now],
    )?;
    mark_fully_delivered(&transaction, id, now)?;
    transaction.commit()?;
    Ok(())
}

fn mark_matrix_failed(connection: &Connection, id: i64, error: &str, retry_at: i64) -> Result<()> {
    connection.execute(
        "UPDATE events SET matrix_attempts=matrix_attempts+1,
         matrix_next_attempt_at=?2, last_matrix_error=?3 WHERE id=?1",
        params![id, retry_at, error],
    )?;
    Ok(())
}

fn mark_fully_delivered(connection: &Connection, id: i64, now: i64) -> Result<()> {
    connection.execute(
        "UPDATE events SET fully_delivered_at=?2
         WHERE id=?1 AND dead_letter=0
           AND (influx_lines='[]' OR influx_delivered_at IS NOT NULL)
           AND (matrix_room IS NULL OR matrix_delivered_at IS NOT NULL)",
        params![id, now],
    )?;
    Ok(())
}

fn stats(connection: &Connection, now: i64) -> Result<QueueStats> {
    let (pending, oldest, retries): (i64, Option<i64>, i64) = connection.query_row(
        "SELECT COUNT(*), MIN(received_at),
                COALESCE(SUM(influx_attempts + matrix_attempts), 0)
         FROM events WHERE dead_letter=0 AND (
           (influx_lines != '[]' AND influx_delivered_at IS NULL) OR
           (matrix_room IS NOT NULL AND matrix_delivered_at IS NULL)
         )",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let dead_letters: i64 = connection.query_row(
        "SELECT COUNT(*) FROM events WHERE dead_letter=1",
        [],
        |row| row.get(0),
    )?;
    Ok(QueueStats {
        pending: pending as u64,
        oldest_pending_seconds: oldest.map(|timestamp| now.saturating_sub(timestamp) as u64),
        retries: retries as u64,
        dead_letters: dead_letters as u64,
    })
}

fn prune(connection: &Connection, before: i64) -> Result<u64> {
    Ok(connection.execute(
        "DELETE FROM events WHERE dead_letter=0 AND fully_delivered_at IS NOT NULL
         AND fully_delivered_at < ?1",
        [before],
    )? as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(cursor: &str) -> EventRecord {
        EventRecord {
            machine: "machine-a".into(),
            cursor: Some(cursor.into()),
            raw_json: "{\"event\":\"run\"}".into(),
            received_at: 100,
            influx_lines: vec!["measurement value=1i 1".into()],
            matrix: Some(MatrixOutput {
                room: "!room:example.org".into(),
                body: "machine-a: Starting".into(),
                transaction_id: "qs-monitor-test".into(),
            }),
            processing_error: None,
            dead_letter: false,
        }
    }

    #[tokio::test]
    async fn commit_advances_cursor_and_keeps_each_sink_pending() {
        let temp = tempfile::tempdir().unwrap();
        let (db, actor) = DatabaseActor::open(&temp.path().join("state.sqlite")).unwrap();
        let task = tokio::spawn(actor.run());
        db.commit(record("epoch:1")).await.unwrap();
        // Replaying after a crash immediately after commit is absorbed by the
        // machine/cursor uniqueness constraint rather than duplicating work.
        db.commit(record("epoch:1")).await.unwrap();
        assert_eq!(
            db.cursor("machine-a").await.unwrap().as_deref(),
            Some("epoch:1")
        );

        let influx = db.influx_batch(10, 100).await.unwrap().unwrap();
        assert_eq!(influx.ids.len(), 1);
        // A crash after the external write but before acknowledgement yields
        // the same durable batch, which is safe for identical Influx points.
        let replayed = db.influx_batch(10, 100).await.unwrap().unwrap();
        assert_eq!(replayed.ids, influx.ids);
        assert_eq!(replayed.body, influx.body);
        db.mark_influx_failed(influx.ids.clone(), "offline".into(), 105)
            .await
            .unwrap();
        assert!(db.influx_batch(10, 104).await.unwrap().is_none());
        let stats = db.stats(104).await.unwrap();
        assert_eq!(stats.retries, 1);
        assert_eq!(stats.oldest_pending_seconds, Some(4));
        let influx = db.influx_batch(10, 105).await.unwrap().unwrap();
        db.mark_influx_delivered(influx.ids, 106).await.unwrap();
        assert_eq!(db.stats(107).await.unwrap().pending, 1);

        let matrix = db.matrix_delivery(107).await.unwrap().unwrap();
        let replayed_matrix = db.matrix_delivery(107).await.unwrap().unwrap();
        assert_eq!(replayed_matrix.transaction_id, matrix.transaction_id);
        db.mark_matrix_failed(matrix.id, "offline".into(), 110)
            .await
            .unwrap();
        assert!(db.matrix_delivery(109).await.unwrap().is_none());
        let retried_matrix = db.matrix_delivery(110).await.unwrap().unwrap();
        assert_eq!(retried_matrix.transaction_id, matrix.transaction_id);
        assert_eq!(db.stats(110).await.unwrap().retries, 2);
        db.mark_matrix_delivered(matrix.id, 111).await.unwrap();
        assert_eq!(db.stats(112).await.unwrap().pending, 0);
        db.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
    }

    #[test]
    fn exclusive_lock_rejects_a_second_actor() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let (_db, _actor) = DatabaseActor::open(&path).unwrap();
        assert!(DatabaseActor::open(&path).is_err());
    }

    #[tokio::test]
    async fn dead_letters_are_visible_and_never_pruned() {
        let temp = tempfile::tempdir().unwrap();
        let (db, actor) = DatabaseActor::open(&temp.path().join("state.sqlite")).unwrap();
        let task = tokio::spawn(actor.run());
        let mut event = record("epoch:2");
        event.influx_lines.clear();
        event.matrix = None;
        event.dead_letter = true;
        event.processing_error = Some("malformed".into());
        db.commit(event).await.unwrap();
        assert_eq!(db.stats(200).await.unwrap().dead_letters, 1);
        assert_eq!(db.prune(1000).await.unwrap(), 0);
        db.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn reopening_runs_migrations_without_losing_pending_work() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let (db, actor) = DatabaseActor::open(&path).unwrap();
        let task = tokio::spawn(actor.run());
        db.commit(record("epoch:3")).await.unwrap();
        db.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        drop(connection);

        let (db, actor) = DatabaseActor::open(&path).unwrap();
        let task = tokio::spawn(actor.run());
        assert_eq!(
            db.cursor("machine-a").await.unwrap().as_deref(),
            Some("epoch:3")
        );
        assert!(db.influx_batch(10, 200).await.unwrap().is_some());
        assert!(db.matrix_delivery(200).await.unwrap().is_some());
        db.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
        let connection = Connection::open(&path).unwrap();
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION + 1);
    }
}
