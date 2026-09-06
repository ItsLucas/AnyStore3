//! Changes feed port.

use anystore_domain::change::ChangeRecord;
use anystore_domain::error::DomainResult;
use anystore_domain::ids::CursorId;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub struct ReadChanges {
    /// `None` starts from the oldest retained Change.
    pub cursor: Option<CursorId>,
    pub limit: u32,
    pub now: DateTime<Utc>,
    /// Lifetime granted to the `next_cursor` this read returns.
    pub cursor_expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ChangePage {
    pub items: Vec<ChangeRecord>,
    /// Always present: clients must be able to keep polling, including after an
    /// empty page.
    pub next_cursor: CursorId,
    pub has_more: bool,
    /// Number of committed Changes after this page at read time.
    pub lag: u64,
}

#[async_trait]
pub trait ChangeStore: Send + Sync {
    /// Reads one page of Changes.
    ///
    /// Re-reading a materialised cursor with the same limit must return exactly
    /// the same page, even after newer Changes have been appended.
    async fn read_changes(&self, req: ReadChanges) -> DomainResult<ChangePage>;
}
