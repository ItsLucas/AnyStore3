//! Port conformance suites.
//!
//! The same assertions run against every implementation of a port. This is what
//! makes the abstraction real: replacing `PostgresMetaStore` or
//! `TencentCosBlobStore` cannot change observable behaviour without a failing
//! test here.

pub mod blobstore;
pub mod metastore;
