//! URL signing for the development blob transport.

use hmac::{Hmac, Mac};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const COMPONENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'&')
    .add(b'+')
    .add(b'/')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'%')
    .add(b'\\')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

pub fn encode_component(value: &str) -> String {
    utf8_percent_encode(value, COMPONENT).to_string()
}

/// Signs the method, path, expiry and optional download filename together, so
/// none of them can be swapped independently.
pub fn sign(secret: &[u8], method: &str, path: &str, expires: i64, filename: Option<&str>) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(method.as_bytes());
    mac.update(b"\n");
    mac.update(path.as_bytes());
    mac.update(b"\n");
    mac.update(expires.to_string().as_bytes());
    mac.update(b"\n");
    mac.update(filename.unwrap_or("").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time comparison of signatures.
pub fn verify(
    secret: &[u8],
    method: &str,
    path: &str,
    expires: i64,
    filename: Option<&str>,
    presented: &str,
) -> bool {
    let expected = sign(secret, method, path, expires, filename);
    if expected.len() != presented.len() {
        return false;
    }
    expected
        .bytes()
        .zip(presented.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signatures_round_trip() {
        let sig = sign(b"secret", "GET", "blobs/u1", 100, Some("a.pdf"));
        assert!(verify(b"secret", "GET", "blobs/u1", 100, Some("a.pdf"), &sig));
    }

    #[test]
    fn every_signed_field_is_bound() {
        let sig = sign(b"secret", "GET", "blobs/u1", 100, Some("a.pdf"));
        assert!(!verify(b"secret", "PUT", "blobs/u1", 100, Some("a.pdf"), &sig));
        assert!(!verify(b"secret", "GET", "blobs/u2", 100, Some("a.pdf"), &sig));
        assert!(!verify(b"secret", "GET", "blobs/u1", 101, Some("a.pdf"), &sig));
        assert!(!verify(b"secret", "GET", "blobs/u1", 100, Some("b.pdf"), &sig));
        assert!(!verify(b"other", "GET", "blobs/u1", 100, Some("a.pdf"), &sig));
    }

    #[test]
    fn unicode_filenames_are_encoded() {
        assert_eq!(encode_component("报告 2026.pdf"), "%E6%8A%A5%E5%91%8A%202026.pdf");
    }
}
