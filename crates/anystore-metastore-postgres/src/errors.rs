//! Maps backend errors onto the AnyStore error taxonomy.

use anystore_domain::error::DomainError;

/// Unique index guarding `(parent_id, name)` for live objects.
pub const LIVE_NAME_UNIQUE: &str = "objects_live_parent_name_uq";

pub fn map_sqlx(err: sqlx::Error) -> DomainError {
    if let sqlx::Error::Database(db) = &err
        && db.constraint() == Some(LIVE_NAME_UNIQUE)
    {
        return DomainError::NameConflict;
    }
    DomainError::Internal(Box::new(err))
}

/// True when the error is the live `(parent_id, name)` uniqueness violation.
pub fn is_name_conflict(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Database(db) if db.constraint() == Some(LIVE_NAME_UNIQUE))
}
