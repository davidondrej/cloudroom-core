use super::{Receipt, Record, Session};
use crate::config::Config;
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};
use std::{io, str::FromStr, time::Duration};
use tokio_stream::StreamExt;

pub struct History {
    pub(super) pool: PgPool,
    store: String,
}

impl History {
    pub fn new(config: &Config) -> io::Result<Self> {
        let mut options = PgConnectOptions::from_str(&config.database_url)
            .map_err(|_| io::Error::other("invalid CLOUDROOM_DATABASE_URL"))?;
        if config.allow_insecure_database {
            if !matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1") {
                return Err(io::Error::other(
                    "insecure database access is restricted to loopback tests",
                ));
            }
            options = options.ssl_mode(PgSslMode::Disable);
        } else {
            options = options.ssl_mode(PgSslMode::VerifyFull);
        }
        // Many sandboxes share one database pooler: release idle connections quickly.
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .idle_timeout(Duration::from_secs(60))
            .acquire_timeout(Duration::from_secs(2))
            .connect_lazy_with(options);
        Ok(Self {
            pool,
            store: config.store.clone(),
        })
    }

    pub async fn ready(&self) -> bool {
        // Check the real connection and both required tables without creating test records.
        sqlx::query("SELECT 1 FROM cloudroom_records WHERE store=$1 LIMIT 0")
            .bind(&self.store)
            .execute(&self.pool)
            .await
            .is_ok()
            && sqlx::query("SELECT 1 FROM cloudroom_diagnostics WHERE store=$1 LIMIT 0")
                .bind(&self.store)
                .execute(&self.pool)
                .await
                .is_ok()
    }

    /// One statement per batch: a distant database costs one round trip, not one per record.
    pub async fn upload(&self, records: &[Record]) -> Result<(), sqlx::Error> {
        let texts = records
            .iter()
            .map(for_database)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| sqlx::Error::Decode(e.into()))?;
        let sessions: Vec<&str> = records.iter().map(|r| r.session_id.as_str()).collect();
        let sequences: Vec<i64> = records.iter().map(|r| r.sequence as i64).collect();
        // A lost commit reply may cause retransmission. Only identical content is a retry, or the
        // same row an older Core saved with its native line (a retry across an upgrade).
        // Insert-only, so the database login never needs permission to change history.
        // The final SELECT sees rows from before this statement, so rows it inserted are excluded by `added`.
        let conflicts: i64 = sqlx::query_scalar(
            "WITH incoming AS (SELECT * FROM UNNEST($2::text[], $3::bigint[], $4::text[]) AS i(session_id, sequence, record)), \
             added AS (INSERT INTO cloudroom_records (store, session_id, sequence, record) \
               SELECT $1, session_id, sequence, record FROM incoming \
               ON CONFLICT (store, session_id, sequence) DO NOTHING RETURNING session_id, sequence) \
             SELECT count(*) FROM incoming i \
             WHERE NOT EXISTS (SELECT 1 FROM added a WHERE a.session_id=i.session_id AND a.sequence=i.sequence) \
               AND NOT EXISTS (SELECT 1 FROM cloudroom_records r WHERE r.store=$1 AND r.session_id=i.session_id \
                 AND r.sequence=i.sequence AND (r.record=i.record \
                   OR starts_with(r.record, left(i.record, -1) || ',\"native\":')))")
            .bind(&self.store).bind(&sessions).bind(&sequences).bind(&texts)
            .fetch_one(&self.pool).await?;
        if conflicts > 0 {
            return Err(sqlx::Error::Protocol("conflicting history record".into()));
        }
        Ok(())
    }

    /// Saves a cloud agent's Cloudroom bug report (ADR 0158). The login may only insert reports for its own user.
    pub async fn report(
        &self,
        message: &str,
        context: &serde_json::Value,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO cloudroom_bug_reports (user_id, origin, message, context) VALUES ($1::uuid, 'cloud', $2, $3::jsonb)")
            .bind(&self.store).bind(message).bind(context.to_string())
            .execute(&self.pool).await.map(drop)
    }

    pub async fn summary(
        &self,
        id: &str,
        request: Option<&str>,
    ) -> Result<(Option<Session>, Option<Receipt>), sqlx::Error> {
        let mut receipt = None; // Decode only the latest matching receipt, not superseded versions.
        let mut session = Session {
            session_id: id.into(),
            state: "saved_history_only".into(),
            ..Session::default()
        };
        // Stream metadata scans in Rust: native text can contain NUL, which PostgreSQL JSON
        // processing rejects. Replay's page limit must not truncate identity or status.
        let mut rows = sqlx::query("SELECT record FROM cloudroom_records WHERE store=$1 AND session_id=$2 ORDER BY sequence")
            .bind(&self.store).bind(id).fetch(&self.pool);
        while let Some(row) = rows.next().await {
            let record: Record = serde_json::from_str(row?.get("record"))
                .map_err(|e| sqlx::Error::Decode(e.into()))?;
            session.last_sequence = record.sequence;
            if session.native_id.is_none() && record.kind == "native_identity" {
                session.native_id = record.data["id"].as_str().map(str::to_owned);
            }
            if record.kind == "receipt" && record.data["command"] == "start" {
                session.workspace = record
                    .data
                    .get("workspace")
                    .filter(|w| !w.is_null())
                    .map(|w| serde_json::from_value(w.clone()))
                    .transpose()
                    .map_err(|e| sqlx::Error::Decode(e.into()))?;
                session.harness = serde_json::from_value(
                    record.data["input"]
                        .get("harness")
                        .cloned()
                        .unwrap_or(serde_json::json!("codex")),
                )
                .map_err(|e| sqlx::Error::Decode(e.into()))?;
            }
            if record.kind == "workspace" {
                session.workspace = Some(
                    serde_json::from_value(record.data.clone())
                        .map_err(|e| sqlx::Error::Decode(e.into()))?,
                );
            }
            if record.kind == "receipt" && request.is_some_and(|id| record.data["request_id"] == id)
            {
                receipt = Some(record.data);
            }
        }
        let receipt = receipt
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| sqlx::Error::Decode(e.into()))?;
        Ok(((session.last_sequence > 0).then_some(session), receipt))
    }

    pub async fn read(&self, session: &str, after: u64) -> Result<Vec<Record>, sqlx::Error> {
        let rows = sqlx::query("SELECT record FROM cloudroom_records WHERE store=$1 AND session_id=$2 AND sequence>$3 ORDER BY sequence LIMIT 256")
            .bind(&self.store).bind(session).bind(after as i64).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                serde_json::from_str(r.get::<&str, _>("record"))
                    .map_err(|e| sqlx::Error::Decode(e.into()))
            })
            .collect()
    }
}

/// What the database keeps of a record (ADR 0203). The sandbox's journal keeps every native line, but the
/// database drops those nobody reads from it: copied session-file lines (the harness's own file stays on disk),
/// and Claude's raw output, which Core already turns into the records the app shows. Codex and Pi keep theirs.
fn for_database(record: &Record) -> serde_json::Result<String> {
    if record.native.is_none()
        || (record.kind != "native_record" && record.data["harness"] != "claude-code")
    {
        return serde_json::to_string(record);
    }
    serde_json::to_string(&Record {
        native: None,
        ..record.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn database_drops_unread_native_lines_and_still_matches_older_rows() {
        let record = |kind: &str, data, native: &str| Record {
            sequence: 7,
            session_id: "cr_a".into(),
            kind: kind.into(),
            timestamp_ms: Some(1),
            data,
            native: Some(native.into()),
        };
        let claude = record(
            "text_delta",
            json!({"harness":"claude-code","delta":"hi"}),
            r#"{"type":"stream_event"}"#,
        );
        let file = record(
            "native_record",
            json!({"offset":0}),
            "{\"type\":\"user\"}\n",
        );
        let codex = record(
            "text_delta",
            json!({"method":"item/agentMessage/delta"}),
            "{}",
        );
        for kept in [&claude, &file] {
            let (full, saved) = (
                serde_json::to_string(kept).unwrap(),
                for_database(kept).unwrap(),
            );
            assert!(!saved.contains("\"native\""));
            // The upload's retry check depends on this: an older Core's row is this one plus its native line.
            assert!(full.starts_with(&format!("{},\"native\":", &saved[..saved.len() - 1])));
        }
        assert_eq!(
            for_database(&codex).unwrap(),
            serde_json::to_string(&codex).unwrap()
        );
    }
}
