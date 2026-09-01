//! The response that a mutation commits alongside its object change.
//!
//! Idempotent replay must return the *original* status and body byte-for-byte.
//! Modelling the response as opaque bytes rendered inside the mutation
//! transaction is what makes that guarantee structural rather than incidental.

use anystore_domain::error::DomainResult;
use std::sync::Arc;

use crate::commands::MutationOutcome;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl StoredResponse {
    pub fn json(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body,
        }
    }

    pub fn no_content() -> Self {
        Self {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }
}

/// Renders the public response for a committed mutation.
///
/// Supplied by the application layer and invoked by the store *inside* the
/// mutation transaction, so the persisted idempotency record and the response
/// returned to the caller are always identical.
pub type ResponseRenderer =
    Arc<dyn Fn(&MutationOutcome) -> DomainResult<StoredResponse> + Send + Sync>;
