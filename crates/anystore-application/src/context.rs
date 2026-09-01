//! Per-request context and idempotency request hashing.

use anystore_domain::object::Revision;
use anystore_domain::{IdempotencyKey, PrincipalId, RequestId};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

/// Everything a service needs to know about the caller and the attempt.
#[derive(Clone, Debug)]
pub struct RequestContext {
    pub request_id: RequestId,
    pub principal_id: PrincipalId,
    /// Route template used as the idempotency operation scope.
    pub operation: String,
    pub idempotency_key: Option<IdempotencyKey>,
    /// SHA-256 of the canonical request representation.
    pub request_hash: String,
    pub if_match: Option<Revision>,
    pub now: DateTime<Utc>,
}

/// Hashes the canonical request representation.
///
/// Covers the principal, method, route template, target resource, normalised
/// query parameters, body and `If-Match`. The `Idempotency-Key` itself is
/// deliberately excluded. Each component is length-prefixed so that no two
/// different requests can produce the same canonical string.
pub fn canonical_request_hash(
    principal: &PrincipalId,
    method: &str,
    route: &str,
    target: &str,
    query: &[(String, String)],
    body: &[u8],
    if_match: Option<Revision>,
) -> String {
    let mut sorted: Vec<&(String, String)> = query.iter().collect();
    sorted.sort();

    let mut hasher = Sha256::new();
    let mut field = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    };

    field(principal.as_str().as_bytes());
    field(method.as_bytes());
    field(route.as_bytes());
    field(target.as_bytes());
    field(&(sorted.len() as u64).to_be_bytes());
    for (key, value) in sorted {
        field(key.as_bytes());
        field(value.as_bytes());
    }
    field(body);
    match if_match {
        Some(rev) => field(rev.get().to_string().as_bytes()),
        None => field(b""),
    }

    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(target: &str, body: &[u8], if_match: Option<Revision>) -> String {
        canonical_request_hash(
            &PrincipalId::local(),
            "PATCH",
            "/objects/{id}",
            target,
            &[],
            body,
            if_match,
        )
    }

    #[test]
    fn identical_requests_hash_identically() {
        assert_eq!(hash("obj_1", b"{}", None), hash("obj_1", b"{}", None));
    }

    #[test]
    fn different_targets_hash_differently() {
        assert_ne!(hash("obj_1", b"{}", None), hash("obj_2", b"{}", None));
    }

    #[test]
    fn if_match_participates_in_the_hash() {
        assert_ne!(
            hash("obj_1", b"{}", None),
            hash("obj_1", b"{}", Some(Revision(3)))
        );
    }

    #[test]
    fn field_boundaries_cannot_be_shifted() {
        // Without length prefixes these two would canonicalise identically.
        let a = hash("obj_1", b"ab", None);
        let b = hash("obj_1a", b"b", None);
        assert_ne!(a, b);
    }

    #[test]
    fn query_parameter_order_does_not_matter() {
        let one = canonical_request_hash(
            &PrincipalId::local(),
            "DELETE",
            "/objects/{id}",
            "obj_1",
            &[("a".into(), "1".into()), ("b".into(), "2".into())],
            b"",
            None,
        );
        let two = canonical_request_hash(
            &PrincipalId::local(),
            "DELETE",
            "/objects/{id}",
            "obj_1",
            &[("b".into(), "2".into()), ("a".into(), "1".into())],
            b"",
            None,
        );
        assert_eq!(one, two);
    }
}
