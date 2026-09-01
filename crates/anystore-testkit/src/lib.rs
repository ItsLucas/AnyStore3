//! Test adapters and port conformance suites.
//!
//! The conformance suites are the mechanism the architecture relies on to
//! validate the abstractions: the same tests run against every implementation
//! of a port, so swapping `PostgresMetaStore` for another backend cannot
//! silently change behaviour.

pub mod blobstore;
pub mod conformance;
pub mod metastore;

pub use blobstore::InMemoryBlobStore;
pub use metastore::InMemoryMetaStore;
