//! Deployment-tunable behaviour.

use chrono::Duration;

#[derive(Clone, Debug)]
pub struct AppConfig {
    /// Minimum 24 hours per the API contract.
    pub idempotency_retention: Duration,
    /// How long one attempt may hold a key before another may take over.
    pub idempotency_lease: Duration,
    /// Minimum 30 days per the API contract.
    pub changes_retention: Duration,
    /// Lifetime of a Changes cursor.
    pub changes_cursor_ttl: Duration,
    pub upload_expiry: Duration,
    pub upload_url_expiry: Duration,
    pub download_url_expiry: Duration,
    /// Uploads larger than this use multipart.
    pub multipart_threshold: u64,
    pub part_size: u64,
    pub max_part_numbers: usize,
    /// Grace period before a superseded blob may be deleted.
    pub gc_grace: Duration,
    pub changes_default_limit: u32,
    pub changes_max_limit: u32,
    pub list_default_limit: u32,
    pub list_max_limit: u32,
    pub query_default_limit: u32,
    pub query_max_limit: u32,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            idempotency_retention: Duration::hours(24),
            idempotency_lease: Duration::seconds(30),
            changes_retention: Duration::days(30),
            changes_cursor_ttl: Duration::days(30),
            upload_expiry: Duration::hours(24),
            upload_url_expiry: Duration::hours(6),
            download_url_expiry: Duration::seconds(600),
            multipart_threshold: 64 * 1024 * 1024,
            part_size: 16 * 1024 * 1024,
            max_part_numbers: 1000,
            gc_grace: Duration::hours(1),
            changes_default_limit: 100,
            changes_max_limit: 1000,
            list_default_limit: 100,
            list_max_limit: 1000,
            query_default_limit: 100,
            query_max_limit: 1000,
        }
    }
}
