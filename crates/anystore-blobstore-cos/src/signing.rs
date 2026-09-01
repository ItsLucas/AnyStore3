//! Tencent COS v5 request signing.
//!
//! Implements the documented HMAC-SHA1 scheme:
//!
//! ```text
//! KeyTime      = start;end
//! SignKey      = HMAC-SHA1(SecretKey, KeyTime)                 (lowercase hex)
//! HttpString   = method\npath\nparameters\nheaders\n
//! StringToSign = sha1\nKeyTime\nSHA1(HttpString)\n
//! Signature    = HMAC-SHA1(SignKey, StringToSign)              (lowercase hex)
//! ```
//!
//! This module is pure: it performs no I/O and knows nothing about AnyStore.

use hmac::{Hmac, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha1::{Digest, Sha1};

type HmacSha1 = Hmac<Sha1>;

/// COS `UrlEncode`: everything except the unreserved set is encoded.
const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

pub fn url_encode(value: &str) -> String {
    utf8_percent_encode(value, UNRESERVED).to_string()
}

/// Encodes a path, preserving `/` as the segment separator.
pub fn encode_path(path: &str) -> String {
    path.split('/')
        .map(url_encode)
        .collect::<Vec<_>>()
        .join("/")
}

fn sha1_hex(input: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

fn hmac_sha1_hex(key: &[u8], message: &str) -> String {
    let mut mac = HmacSha1::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Sorted, encoded `key=value` pairs plus the matching key list.
fn canonicalize(pairs: &[(String, String)]) -> (String, String) {
    let mut encoded: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (url_encode(&k.to_ascii_lowercase()), url_encode(v)))
        .collect();
    encoded.sort();

    let joined = encoded
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let list = encoded
        .iter()
        .map(|(k, _)| k.clone())
        .collect::<Vec<_>>()
        .join(";");

    (list, joined)
}

#[derive(Clone, Debug)]
pub struct SignatureInput<'a> {
    pub method: &'a str,
    /// Raw, *unencoded* object path, e.g. `/blobs/upload_01`.
    pub path: &'a str,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    pub key_time: String,
    pub header_list: String,
    pub url_param_list: String,
    pub signature: String,
}

impl Signature {
    /// Renders the value used both for `Authorization` and for the signed query
    /// string. Query use additionally percent-encodes `;` in the timestamps.
    pub fn render(&self, secret_id: &str, encode_semicolons: bool) -> String {
        let key_time = if encode_semicolons {
            self.key_time.replace(';', "%3B")
        } else {
            self.key_time.clone()
        };
        format!(
            "q-sign-algorithm=sha1&q-ak={secret_id}&q-sign-time={key_time}&q-key-time={key_time}\
             &q-header-list={}&q-url-param-list={}&q-signature={}",
            self.header_list, self.url_param_list, self.signature
        )
    }
}

pub fn sign(secret_key: &str, input: &SignatureInput<'_>) -> Signature {
    let key_time = format!("{};{}", input.start, input.end);
    let sign_key = hmac_sha1_hex(secret_key.as_bytes(), &key_time);

    let (url_param_list, http_parameters) = canonicalize(&input.query);
    let (header_list, http_headers) = canonicalize(&input.headers);

    let http_string = format!(
        "{}\n{}\n{}\n{}\n",
        input.method.to_ascii_lowercase(),
        input.path,
        http_parameters,
        http_headers
    );

    let string_to_sign = format!("sha1\n{key_time}\n{}\n", sha1_hex(&http_string));
    let signature = hmac_sha1_hex(sign_key.as_bytes(), &string_to_sign);

    Signature {
        key_time,
        header_list,
        url_param_list,
        signature,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Official worked example: GET with response-* parameters.
    /// https://cloud.tencent.com/document/product/436/7778
    #[test]
    fn matches_the_official_get_example() {
        let http_string = concat!(
            "get\n/exampleobject(腾讯云)\n",
            "response-cache-control=max-age%3D600&response-content-type=application%2Foctet-stream\n",
            "date=Thu%2C%2016%20May%202019%2006%3A55%3A53%20GMT&host=examplebucket-1250000000.cos.ap-beijing.myqcloud.com\n"
        );
        assert_eq!(
            sha1_hex(http_string),
            "54ecfe22f59d3514fdc764b87a32d8133ea611e6"
        );

        let sign_key = "937914bf490e9e8c189836aad2052e4feeb35eaf";
        let string_to_sign =
            "sha1\n1557989753;1557996953\n54ecfe22f59d3514fdc764b87a32d8133ea611e6\n";
        let signature = hmac_sha1_hex(sign_key.as_bytes(), string_to_sign);
        // The published signature masks its final characters.
        assert!(
            signature.starts_with("01681b8c9d798a678e43b685a9f1bba0f6c0"),
            "unexpected signature {signature}"
        );
    }

    /// Official worked example: PUT with several signed headers.
    #[test]
    fn matches_the_official_put_example() {
        let http_string = concat!(
            "put\n/exampleobject(腾讯云)\n\n",
            "content-length=13&content-md5=mQ%2FfVh815F3k6TAUm8m0eg%3D%3D&content-type=text%2Fplain",
            "&date=Thu%2C%2016%20May%202019%2006%3A45%3A51%20GMT",
            "&host=examplebucket-1250000000.cos.ap-beijing.myqcloud.com",
            "&x-cos-acl=private&x-cos-grant-read=uin%3D%22100000000011%22\n"
        );
        assert_eq!(
            sha1_hex(http_string),
            "8b2751e77f43a0995d6e9eb9477f4b685cca4172"
        );

        let sign_key = "eb2519b498b02ac213cb1f3d1a3d27a3b3c9bc5f";
        let string_to_sign =
            "sha1\n1557989151;1557996351\n8b2751e77f43a0995d6e9eb9477f4b685cca4172\n";
        let signature = hmac_sha1_hex(sign_key.as_bytes(), string_to_sign);
        assert!(
            signature.starts_with("3b8851a11a569213c17ba8fa7dcf2abec693"),
            "unexpected signature {signature}"
        );
    }

    /// Reproduces the documented canonicalisation of headers and parameters.
    #[test]
    fn canonicalisation_matches_the_documented_example() {
        let headers = vec![
            ("Content-Length".into(), "13".into()),
            ("Content-MD5".into(), "mQ/fVh815F3k6TAUm8m0eg==".into()),
            ("Content-Type".into(), "text/plain".into()),
            ("Date".into(), "Thu, 16 May 2019 06:45:51 GMT".into()),
            (
                "Host".into(),
                "examplebucket-1250000000.cos.ap-beijing.myqcloud.com".into(),
            ),
            ("x-cos-acl".into(), "private".into()),
            ("x-cos-grant-read".into(), "uin=\"100000000011\"".into()),
        ];
        let (list, joined) = canonicalize(&headers);
        assert_eq!(
            list,
            "content-length;content-md5;content-type;date;host;x-cos-acl;x-cos-grant-read"
        );
        assert_eq!(
            joined,
            "content-length=13&content-md5=mQ%2FfVh815F3k6TAUm8m0eg%3D%3D&content-type=text%2Fplain\
             &date=Thu%2C%2016%20May%202019%2006%3A45%3A51%20GMT\
             &host=examplebucket-1250000000.cos.ap-beijing.myqcloud.com\
             &x-cos-acl=private&x-cos-grant-read=uin%3D%22100000000011%22"
        );
    }

    #[test]
    fn parameters_are_sorted_and_encoded() {
        let query = vec![
            ("max-keys".into(), "10".into()),
            ("prefix".into(), "example-folder/".into()),
            ("delimiter".into(), "/".into()),
        ];
        let (list, joined) = canonicalize(&query);
        assert_eq!(list, "delimiter;max-keys;prefix");
        assert_eq!(joined, "delimiter=%2F&max-keys=10&prefix=example-folder%2F");
    }

    #[test]
    fn end_to_end_signature_is_deterministic() {
        let input = SignatureInput {
            method: "PUT",
            path: "/blobs/upload_01",
            query: vec![],
            headers: vec![("host".into(), "b-125.cos.ap-beijing.myqcloud.com".into())],
            start: 1_557_989_151,
            end: 1_557_996_351,
        };
        let a = sign("secret", &input);
        let b = sign("secret", &input);
        assert_eq!(a, b);
        assert_eq!(a.header_list, "host");
        assert_eq!(a.url_param_list, "");
        assert_eq!(a.key_time, "1557989151;1557996351");
    }

    #[test]
    fn query_rendering_encodes_semicolons() {
        let signature = Signature {
            key_time: "1;2".into(),
            header_list: "host".into(),
            url_param_list: String::new(),
            signature: "deadbeef".into(),
        };
        assert!(signature.render("ak", true).contains("q-sign-time=1%3B2"));
        assert!(signature.render("ak", false).contains("q-sign-time=1;2"));
    }

    #[test]
    fn paths_encode_segments_but_keep_separators() {
        assert_eq!(encode_path("/blobs/upload 01"), "/blobs/upload%2001");
        assert_eq!(encode_path("/a~b_c.d-e"), "/a~b_c.d-e");
    }
}
