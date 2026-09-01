//! Metadata query.

use anystore_domain::error::DomainResult;
use anystore_domain::object::ObjectKind;
use anystore_domain::page::clamp_limit;
use anystore_domain::ObjectId;
use anystore_metastore::commands::{MetadataCondition, ObjectQuery};
use serde_json::Value;
use std::sync::Arc;

use crate::dto::{object_json, page_json};
use crate::state::AppState;

#[derive(Clone, Debug, Default)]
pub struct QueryRequest {
    pub kind: Option<ObjectKind>,
    pub name: Option<String>,
    pub parent_id: Option<ObjectId>,
    /// Top-level metadata conditions, all ANDed.
    pub metadata: Vec<(String, MetadataCondition)>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone)]
pub struct QueryService {
    state: Arc<AppState>,
}

impl QueryService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn query(&self, req: QueryRequest) -> DomainResult<Value> {
        let page = self
            .state
            .meta
            .query_objects(ObjectQuery {
                kind: req.kind,
                name: req.name,
                parent_id: req.parent_id,
                metadata: req.metadata,
                limit: clamp_limit(
                    req.limit,
                    self.state.config.query_default_limit,
                    self.state.config.query_max_limit,
                ),
                cursor: req.cursor,
            })
            .await?;

        Ok(page_json(
            page.items.iter().map(object_json).collect(),
            page.next_cursor.as_deref(),
            page.has_more,
        ))
    }
}
