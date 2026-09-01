//! Path normalisation and rendering.
//!
//! Paths are derived from `parent_id + name`; they are never authoritative
//! state and are never mapped onto a local filesystem path.

use crate::error::{DomainError, DomainResult};
use crate::name::validate_name;

/// Splits an absolute AnyStore path into its segments.
///
/// The root path `/` yields an empty segment list. `.` and `..` are rejected
/// rather than resolved, so a path can never escape the tree.
pub fn split_path(path: &str) -> DomainResult<Vec<String>> {
    if !path.starts_with('/') {
        return Err(DomainError::InvalidRequest(
            "path must start with '/'.".into(),
        ));
    }
    if path.contains('\0') {
        return Err(DomainError::InvalidRequest(
            "path must not contain NUL.".into(),
        ));
    }

    let mut segments = Vec::new();
    for raw in path.split('/') {
        if raw.is_empty() {
            // Tolerates a trailing slash and collapses repeated separators.
            continue;
        }
        validate_name(raw).map_err(|_| {
            DomainError::InvalidRequest(format!("path segment {raw:?} is not a valid name."))
        })?;
        segments.push(raw.to_owned());
    }
    Ok(segments)
}

/// Renders the public `path` value for a chain of names ordered root-first.
pub fn render_path(segments: &[String]) -> String {
    if segments.is_empty() {
        return "/".to_owned();
    }
    let mut out = String::new();
    for segment in segments {
        out.push('/');
        out.push_str(segment);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_path_has_no_segments() {
        assert!(split_path("/").unwrap().is_empty());
        assert_eq!(render_path(&[]), "/");
    }

    #[test]
    fn nested_paths_split_and_render_symmetrically() {
        let segments = split_path("/reports/2026/example.pdf").unwrap();
        assert_eq!(segments, vec!["reports", "2026", "example.pdf"]);
        assert_eq!(render_path(&segments), "/reports/2026/example.pdf");
    }

    #[test]
    fn redundant_separators_are_collapsed() {
        assert_eq!(split_path("//a//b/").unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn traversal_segments_are_rejected() {
        for path in ["/a/../b", "/./a", "/.."] {
            assert!(split_path(path).is_err(), "{path} should be rejected");
        }
    }

    #[test]
    fn relative_paths_are_rejected() {
        assert!(split_path("reports/x").is_err());
    }
}
