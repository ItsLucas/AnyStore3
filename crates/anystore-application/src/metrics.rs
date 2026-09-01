//! Process-local metrics.
//!
//! Lives in the application layer so services can record events the HTTP
//! boundary cannot observe, such as an idempotent replay.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    pub http_requests_total: AtomicU64,
    pub http_request_duration_ms_total: AtomicU64,
    pub revision_conflicts_total: AtomicU64,
    pub idempotency_hits_total: AtomicU64,
    pub idempotency_conflicts_total: AtomicU64,
    pub upload_sessions_total: AtomicU64,
    pub upload_complete_failures_total: AtomicU64,
    pub blob_provider_errors_total: AtomicU64,
    pub db_errors_total: AtomicU64,
    pub changes_lag: AtomicU64,
    pub blob_gc_pending: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, Ordering::Relaxed);
    }

    pub fn set(gauge: &AtomicU64, value: u64) {
        gauge.store(value, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Renders the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let counters: [(&str, &str, &AtomicU64); 9] = [
            ("http_requests_total", "counter", &self.http_requests_total),
            (
                "http_request_duration_ms_total",
                "counter",
                &self.http_request_duration_ms_total,
            ),
            (
                "revision_conflicts_total",
                "counter",
                &self.revision_conflicts_total,
            ),
            (
                "idempotency_hits_total",
                "counter",
                &self.idempotency_hits_total,
            ),
            (
                "idempotency_conflicts_total",
                "counter",
                &self.idempotency_conflicts_total,
            ),
            (
                "upload_sessions_total",
                "counter",
                &self.upload_sessions_total,
            ),
            (
                "upload_complete_failures_total",
                "counter",
                &self.upload_complete_failures_total,
            ),
            (
                "blob_provider_errors_total",
                "counter",
                &self.blob_provider_errors_total,
            ),
            ("db_errors_total", "counter", &self.db_errors_total),
        ];

        let gauges: [(&str, &AtomicU64); 2] = [
            ("changes_lag", &self.changes_lag),
            ("blob_gc_pending", &self.blob_gc_pending),
        ];

        let mut out = String::new();
        for (name, kind, value) in counters {
            out.push_str(&format!("# TYPE anystore_{name} {kind}\n"));
            out.push_str(&format!("anystore_{name} {}\n", Self::get(value)));
        }
        for (name, value) in gauges {
            out.push_str(&format!("# TYPE anystore_{name} gauge\n"));
            out.push_str(&format!("anystore_{name} {}\n", Self::get(value)));
        }
        out
    }
}
