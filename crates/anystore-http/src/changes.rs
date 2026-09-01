//! Changes feed handler.

use anystore_application::changes::ChangeService;
use axum::extract::{RawQuery, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Extension;
use chrono::Utc;
use std::sync::Arc;

use crate::HttpState;
use crate::request::{RequestMeta, find, json_response, parse_limit, query_pairs};
use crate::respond;

pub async fn read(
    State(state): State<HttpState>,
    Extension(meta): Extension<RequestMeta>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let request_id = meta.request_id.clone();
    respond(Arc::clone(&state.app.metrics), &request_id, async move {
        let pairs = query_pairs(raw_query.as_deref());
        let service = ChangeService::new(Arc::clone(&state.app));

        let value = service
            .read(
                find(&pairs, "cursor").map(str::to_owned),
                parse_limit(&pairs)?,
                Utc::now(),
            )
            .await?;

        Ok(json_response(StatusCode::OK, &value, &meta.request_id))
    })
    .await
}
