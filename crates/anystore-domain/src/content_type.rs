//! Content type validation.
//!
//! `content_type` comes from untrusted input and is later emitted as an HTTP
//! header (`HEAD /objects/{id}/content`) and signed into provider requests, so
//! it must be a syntactically valid header value with a `type/subtype` shape.

use crate::error::{DomainError, DomainResult};

/// Maximum content type length in bytes.
pub const MAX_CONTENT_TYPE_BYTES: usize = 255;

/// RFC 9110 `tchar`.
fn is_token_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

fn is_token(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(is_token_char)
}

/// Validates a media type such as `application/pdf` or
/// `text/plain; charset=utf-8`.
///
/// The `type/subtype` pair must be RFC 9110 tokens; parameters are only
/// required to be visible ASCII, spaces or tabs.
pub fn validate_content_type(content_type: &str) -> DomainResult<()> {
    let invalid = || {
        DomainError::InvalidRequest(format!(
            "content_type must be a media type such as application/pdf, \
             at most {MAX_CONTENT_TYPE_BYTES} bytes."
        ))
    };

    if content_type.is_empty() || content_type.len() > MAX_CONTENT_TYPE_BYTES {
        return Err(invalid());
    }
    if !content_type
        .bytes()
        .all(|b| b.is_ascii_graphic() || b == b' ' || b == b'\t')
    {
        return Err(invalid());
    }

    let essence = content_type.split(';').next().unwrap_or_default().trim();
    match essence.split_once('/') {
        Some((kind, subtype)) if is_token(kind) && is_token(subtype) => Ok(()),
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_media_types_with_parameters() {
        for value in [
            "application/pdf",
            "text/plain; charset=utf-8",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "multipart/form-data; boundary=\"a b\"",
        ] {
            assert!(
                validate_content_type(value).is_ok(),
                "{value} should be valid"
            );
        }
    }

    #[test]
    fn rejects_header_injection_and_malformed_values() {
        for value in [
            "",
            "text/plain\nX: 1",
            "text/plain\r",
            "text/\u{e9}",
            "pdf",
            "/pdf",
            "application/",
            "a b/c",
        ] {
            let err = validate_content_type(value).unwrap_err();
            assert_eq!(
                err.code(),
                "invalid_request",
                "{value:?} should be rejected"
            );
        }
        let long = format!("a/{}", "b".repeat(MAX_CONTENT_TYPE_BYTES));
        assert!(validate_content_type(&long).is_err());
    }
}
