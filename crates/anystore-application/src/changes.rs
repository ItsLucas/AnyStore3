//! Changes feed reads.

use anystore_domain::error::DomainResult;
use anystore_domain::ids::CursorId;
use anystore_domain::page::clamp_limit;
use anystore_metastore::changes::ReadChanges;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::dto::change_json;
use crate::state::AppState;

#[derive(Clone)]
pub struct ChangeService {
    state: Arc<AppState>,
}

impl ChangeService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn read(
        &self,
        cursor: Option<String>,
        limit: Option<u32>,
        now: DateTime<Utc>,
    ) -> DomainResult<Value> {
        let config = &self.state.config;
        let page = self
            .state
            .meta
            .read_changes(ReadChanges {
                cursor: cursor.filter(|c| !c.is_empty()).map(CursorId::new),
                limit: clamp_limit(
                    limit,
                    config.changes_default_limit,
                    config.changes_max_limit,
                ),
                now,
                cursor_expires_at: now + config.changes_cursor_ttl,
            })
            .await?;

        Ok(json!({
            "items": page.items.iter().map(change_json).collect::<Vec<_>>(),
            // Always present so clients can keep polling, even after an empty page.
            "next_cursor": page.next_cursor.as_str(),
            "has_more": page.has_more,
        }))
    }
}
