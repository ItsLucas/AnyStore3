//! Changes feed with stable, materialised cursors.
//!
//! The continuation cursor id is minted here so identifier formats stay in the
//! domain layer; the database binds it to the page it just computed.

use anystore_domain::change::ChangeRecord;
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::ids::CursorId;
use anystore_metastore::changes::{ChangePage, ChangeStore, ReadChanges};
use async_trait::async_trait;
use serde_json::json;

use crate::PostgrestMetaStore;
use crate::decode;

#[async_trait]
impl ChangeStore for PostgrestMetaStore {
    async fn read_changes(&self, req: ReadChanges) -> DomainResult<ChangePage> {
        if let Some(cursor) = &req.cursor
            && !cursor.is_well_formed()
        {
            // A malformed value is a bad request, not an expired cursor.
            return Err(DomainError::InvalidRequest("Invalid cursor.".into()));
        }

        let data = self
            .client()
            .call(
                "read_changes",
                json!({
                    "cursor": req.cursor.as_ref().map(|cursor| cursor.as_str()),
                    "limit": req.limit,
                    "now": decode::timestamp(req.now),
                    "cursor_expires_at": decode::timestamp(req.cursor_expires_at),
                    "new_cursor_id": CursorId::generate().as_str(),
                }),
            )
            .await?;

        let items = decode::array(&data, "items")?
            .iter()
            .map(decode::change_record)
            .collect::<DomainResult<Vec<ChangeRecord>>>()?;

        Ok(ChangePage {
            items,
            // Always present: clients must be able to keep polling, including
            // after an empty page.
            next_cursor: CursorId::new(decode::text(&data, "next_cursor")?),
            has_more: decode::boolean(&data, "has_more")?,
            lag: decode::integer(&data, "lag")?,
        })
    }
}
