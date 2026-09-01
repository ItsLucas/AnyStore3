//! Object name validation.
//!
//! Names come from untrusted input and are never mapped to a local filesystem
//! path, so the rules here are purely the ones stated by the API contract.

use crate::error::{DomainError, DomainResult};

/// Maximum name length in bytes. Not mandated by the contract, but a bound is
/// required so that untrusted input cannot exhaust storage or indexes.
pub const MAX_NAME_BYTES: usize = 255;

/// Validates a non-root object name.
///
/// `name` MUST be valid UTF-8 (guaranteed by `&str`), non-empty, free of `/`
/// and NUL, and not equal to `.` or `..`.
pub fn validate_name(name: &str) -> DomainResult<()> {
    if name.is_empty() {
        return Err(DomainError::InvalidName("Name must not be empty.".into()));
    }
    if name.len() > MAX_NAME_BYTES {
        return Err(DomainError::InvalidName(format!(
            "Name must not exceed {MAX_NAME_BYTES} bytes."
        )));
    }
    if name.contains('/') {
        return Err(DomainError::InvalidName("Name must not contain '/'.".into()));
    }
    if name.contains('\0') {
        return Err(DomainError::InvalidName("Name must not contain NUL.".into()));
    }
    if name == "." || name == ".." {
        return Err(DomainError::InvalidName(
            "Name must not be '.' or '..'.".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_ordinary_and_unicode_names() {
        for name in ["example.pdf", "报告 2026.xlsx", "a b\tc", "..."] {
            assert!(validate_name(name).is_ok(), "{name} should be valid");
        }
    }

    #[test]
    fn rejects_contract_violations() {
        for name in ["", ".", "..", "a/b", "a\0b"] {
            let err = validate_name(name).unwrap_err();
            assert_eq!(err.code(), "invalid_name", "{name:?} should be rejected");
        }
    }

    #[test]
    fn rejects_over_long_names() {
        let name = "x".repeat(MAX_NAME_BYTES + 1);
        assert_eq!(validate_name(&name).unwrap_err().code(), "invalid_name");
    }
}
