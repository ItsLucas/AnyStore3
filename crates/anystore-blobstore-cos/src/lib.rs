//! Tencent COS `BlobStore`.
//!
//! Also targets LightCOS, which is COS v5 API compatible; only the endpoint
//! differs. All COS-specific protocol and signing detail is confined to this
//! crate.

mod signing;

pub use signing::{Signature, SignatureInput};

use anystore_blobstore::{
    AbortBlobUpload, BlobRef, BlobStat, BlobStore, CompleteBlobUpload, PrepareUpload,
    PreparedUpload, SignParts, SignedDownload, SignedPart, SignedRequest,
};
use anystore_domain::error::{DomainError, DomainResult};
use anystore_domain::upload::UploadMode;
use async_trait::async_trait;
use chrono::Utc;
use quick_xml::events::Event;
use reqwest::{Client, Method, StatusCode};
use std::collections::BTreeMap;
use std::time::Duration;

pub const BACKEND_ID: &str = "tencent_cos";

/// Clock skew allowance on the signature validity window.
const SIGN_SKEW_SECONDS: i64 = 60;

#[derive(Clone, Debug)]
pub struct CosConfig {
    /// Bucket name including the APPID suffix, e.g. `anystore-1250000000`.
    pub bucket: String,
    /// Service domain, e.g. `cos.ap-shanghai.myqcloud.com` or `light-cos.com`.
    pub endpoint: String,
    pub region: Option<String>,
    pub secret_id: String,
    pub secret_key: String,
    /// Present only when temporary credentials are used.
    pub session_token: Option<String>,
}

pub struct TencentCosBlobStore {
    config: CosConfig,
    host: String,
    client: Client,
}

impl std::fmt::Debug for TencentCosBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render credentials.
        f.debug_struct("TencentCosBlobStore")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

impl TencentCosBlobStore {
    pub fn new(config: CosConfig) -> DomainResult<Self> {
        let endpoint = config
            .endpoint
            .trim()
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned();

        if endpoint.is_empty() || config.bucket.trim().is_empty() {
            return Err(DomainError::internal("COS bucket and endpoint are required"));
        }

        // Tolerates an endpoint that already includes the bucket.
        let host = if endpoint.starts_with(&format!("{}.", config.bucket)) {
            endpoint
        } else {
            format!("{}.{}", config.bucket, endpoint)
        };

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| DomainError::internal(format!("HTTP client build failed: {e}")))?;

        Ok(Self {
            config,
            host,
            client,
        })
    }

    fn object_path(&self, blob: &BlobRef) -> String {
        format!("/{}", blob.as_str())
    }

    fn url(&self, path: &str, query: &[(String, String)]) -> String {
        let mut url = format!("https://{}{}", self.host, signing::encode_path(path));
        if !query.is_empty() {
            url.push('?');
            url.push_str(&render_query(query));
        }
        url
    }

    fn sign(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        valid_for: Duration,
    ) -> Signature {
        let now = Utc::now().timestamp();
        signing::sign(
            &self.config.secret_key,
            &SignatureInput {
                method,
                path,
                query: query.to_vec(),
                // Only `host` is signed, so clients are free to choose their own
                // Content-Type when uploading to a presigned URL.
                headers: vec![("host".to_owned(), self.host.clone())],
                start: now - SIGN_SKEW_SECONDS,
                end: now + valid_for.as_secs() as i64,
            },
        )
    }

    fn presign(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        expires_in: Duration,
    ) -> String {
        let signature = self.sign(method, path, query, expires_in);
        let mut url = self.url(path, query);
        url.push(if query.is_empty() { '?' } else { '&' });
        url.push_str(&signature.render(&self.config.secret_id, true));

        if let Some(token) = &self.config.session_token {
            url.push_str("&x-cos-security-token=");
            url.push_str(&signing::url_encode(token));
        }
        url
    }

    /// Performs a server-side authenticated request.
    async fn send(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        body: Option<Vec<u8>>,
    ) -> DomainResult<(StatusCode, reqwest::header::HeaderMap, Vec<u8>)> {
        let signature = self.sign(method.as_str(), path, query, Duration::from_secs(300));

        let mut request = self
            .client
            .request(method, self.url(path, query))
            .header(
                reqwest::header::AUTHORIZATION,
                signature.render(&self.config.secret_id, false),
            )
            .header(reqwest::header::HOST, &self.host);

        if let Some(token) = &self.config.session_token {
            request = request.header("x-cos-security-token", token);
        }
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/xml")
                .body(body);
        }

        let response = request.send().await.map_err(|e| {
            // The URL carries a signature, so it must never reach a log.
            DomainError::storage(format!("COS request failed: {}", redact(&e.to_string())))
        })?;

        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| DomainError::storage(format!("reading COS response failed: {e}")))?;

        Ok((status, headers, bytes.to_vec()))
    }

    async fn head(&self, blob: &BlobRef) -> DomainResult<Option<BlobStat>> {
        let (status, headers, _) = self
            .send(Method::HEAD, &self.object_path(blob), &[], None)
            .await?;

        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(DomainError::storage(format!(
                "COS stat returned {}",
                status.as_u16()
            )));
        }

        let size = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        Ok(Some(BlobStat {
            size,
            content_type: headers
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            etag: headers
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim_matches('"').to_owned()),
            // COS exposes MD5 and CRC64, never SHA-256, so verification is not
            // possible without downloading the blob.
            sha256: None,
        }))
    }

    async fn initiate_multipart(&self, blob: &BlobRef) -> DomainResult<String> {
        let query = vec![("uploads".to_owned(), String::new())];
        let (status, _, body) = self
            .send(Method::POST, &self.object_path(blob), &query, None)
            .await?;

        if !status.is_success() {
            return Err(cos_error("initiating multipart upload", status, &body));
        }
        extract_tag(&body, "UploadId")
            .ok_or_else(|| DomainError::storage("COS did not return an upload id"))
    }
}

fn render_query(query: &[(String, String)]) -> String {
    query
        .iter()
        .map(|(k, v)| {
            if v.is_empty() {
                signing::url_encode(k)
            } else {
                format!("{}={}", signing::url_encode(k), signing::url_encode(v))
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Strips any signature material that may appear in a provider message.
fn redact(message: &str) -> String {
    match message.find("q-signature=") {
        Some(index) => format!("{}q-signature=<redacted>", &message[..index]),
        None => message.to_owned(),
    }
}

fn cos_error(action: &str, status: StatusCode, body: &[u8]) -> DomainError {
    let code = extract_tag(body, "Code").unwrap_or_else(|| status.as_u16().to_string());
    DomainError::storage(format!("COS {action} failed with {code}"))
}

/// Extracts the text content of the first matching element.
fn extract_tag(xml: &[u8], tag: &str) -> Option<String> {
    let text = std::str::from_utf8(xml).ok()?;
    let mut reader = quick_xml::Reader::from_str(text);
    let mut inside = false;
    let mut collected = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                if element.name().as_ref() == tag {
                    inside = true;
                    collected.clear();
                }
            }
            Ok(Event::Text(content)) if inside => {
                collected.push_str(&content.xml10_content());
            }
            Ok(Event::CData(content)) if inside => {
                collected.push_str(content.as_ref());
            }
            Ok(Event::End(element)) => {
                if element.name().as_ref() == tag && inside {
                    return Some(collected.trim().to_owned());
                }
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn complete_multipart_body(parts: &[anystore_blobstore::CompletedPart]) -> Vec<u8> {
    let mut sorted: Vec<&anystore_blobstore::CompletedPart> = parts.iter().collect();
    sorted.sort_by_key(|p| p.part_number);

    let mut xml = String::from("<CompleteMultipartUpload>");
    for part in sorted {
        xml.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            part.part_number,
            escape_xml(&part.etag)
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml.into_bytes()
}

#[async_trait]
impl BlobStore for TencentCosBlobStore {
    fn backend_id(&self) -> &'static str {
        BACKEND_ID
    }

    fn verifies_sha256(&self) -> bool {
        false
    }

    fn blob_ref_for(&self, upload_id: &str) -> BlobRef {
        // Deliberately excludes the object's folder path and name, so rename
        // and move never touch storage.
        BlobRef::new(format!("blobs/{upload_id}"))
    }

    async fn prepare_upload(&self, req: PrepareUpload) -> DomainResult<PreparedUpload> {
        let blob_ref = self.blob_ref_for(&req.upload_id);
        let path = self.object_path(&blob_ref);
        let expires_at = Utc::now()
            + chrono::Duration::from_std(req.expires_in)
                .unwrap_or_else(|_| chrono::Duration::hours(6));

        match req.mode {
            UploadMode::Single => Ok(PreparedUpload {
                single: Some(SignedRequest {
                    method: "PUT".into(),
                    url: self.presign("PUT", &path, &[], req.expires_in),
                    headers: BTreeMap::new(),
                }),
                part_size: None,
                provider_upload_id: None,
                blob_ref,
                mode: req.mode,
                expires_at,
            }),
            UploadMode::Multipart => {
                let provider_upload_id = self.initiate_multipart(&blob_ref).await?;
                Ok(PreparedUpload {
                    single: None,
                    part_size: Some(req.part_size),
                    provider_upload_id: Some(provider_upload_id),
                    blob_ref,
                    mode: req.mode,
                    expires_at,
                })
            }
        }
    }

    async fn sign_parts(&self, req: SignParts) -> DomainResult<Vec<SignedPart>> {
        let path = self.object_path(&req.blob_ref);
        Ok(req
            .part_numbers
            .iter()
            .map(|number| {
                let query = vec![
                    ("partNumber".to_owned(), number.to_string()),
                    ("uploadId".to_owned(), req.provider_upload_id.clone()),
                ];
                SignedPart {
                    part_number: *number,
                    method: "PUT".into(),
                    url: self.presign("PUT", &path, &query, req.expires_in),
                }
            })
            .collect())
    }

    async fn ensure_upload_completed(&self, req: CompleteBlobUpload) -> DomainResult<BlobStat> {
        let path = self.object_path(&req.blob_ref);

        if req.mode == UploadMode::Multipart {
            let provider_upload_id = req
                .provider_upload_id
                .clone()
                .ok_or_else(|| DomainError::storage("multipart upload has no provider id"))?;

            if req.parts.is_empty() {
                return Err(DomainError::InvalidRequest(
                    "Multipart completion requires the uploaded parts.".into(),
                ));
            }

            let query = vec![("uploadId".to_owned(), provider_upload_id)];
            let (status, _, body) = self
                .send(
                    Method::POST,
                    &path,
                    &query,
                    Some(complete_multipart_body(&req.parts)),
                )
                .await?;

            // COS, like S3, may report a failure inside a 200 response.
            let failed = !status.is_success() || extract_tag(&body, "Code").is_some();
            if failed {
                // The provider upload may already have been finalised before a
                // crash; an existing object means completion has converged.
                if let Some(stat) = self.head(&req.blob_ref).await? {
                    tracing::info!("multipart upload was already finalised; reusing the blob");
                    return Ok(stat);
                }
                return Err(cos_error("completing multipart upload", status, &body));
            }
        }

        self.head(&req.blob_ref)
            .await?
            .ok_or(DomainError::ContentNotReady)
    }

    async fn abort_upload(&self, req: AbortBlobUpload) -> DomainResult<()> {
        let Some(provider_upload_id) = req.provider_upload_id else {
            return Ok(());
        };
        let query = vec![("uploadId".to_owned(), provider_upload_id)];
        let (status, _, body) = self
            .send(Method::DELETE, &self.object_path(&req.blob_ref), &query, None)
            .await?;

        if status.is_success() || status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Err(cos_error("aborting multipart upload", status, &body))
    }

    async fn stat(&self, blob: &BlobRef) -> DomainResult<BlobStat> {
        self.head(blob).await?.ok_or(DomainError::ContentNotReady)
    }

    async fn sign_download(
        &self,
        blob: &BlobRef,
        filename: &str,
        expires_in: Duration,
    ) -> DomainResult<SignedDownload> {
        // Applying the disposition at signing time is what lets a download
        // always present the object's current name.
        let disposition = format!(
            "attachment; filename*=UTF-8''{}",
            signing::url_encode(filename)
        );
        let query = vec![("response-content-disposition".to_owned(), disposition)];

        Ok(SignedDownload {
            url: self.presign("GET", &self.object_path(blob), &query, expires_in),
            expires_at: Utc::now()
                + chrono::Duration::from_std(expires_in)
                    .unwrap_or_else(|_| chrono::Duration::seconds(600)),
        })
    }

    async fn delete_blob(&self, blob: &BlobRef) -> DomainResult<()> {
        let (status, _, body) = self
            .send(Method::DELETE, &self.object_path(blob), &[], None)
            .await?;

        // A missing blob counts as success so garbage collection is idempotent.
        if status.is_success() || status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Err(cos_error("deleting blob", status, &body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anystore_blobstore::CompletedPart;

    fn store() -> TencentCosBlobStore {
        TencentCosBlobStore::new(CosConfig {
            bucket: "anystore-1250000000".into(),
            endpoint: "light-cos.com".into(),
            region: None,
            secret_id: "AKIDEXAMPLE".into(),
            secret_key: "SECRETEXAMPLE".into(),
            session_token: None,
        })
        .unwrap()
    }

    #[test]
    fn host_is_built_from_bucket_and_endpoint() {
        assert_eq!(store().host, "anystore-1250000000.light-cos.com");
    }

    #[test]
    fn endpoint_that_already_contains_the_bucket_is_not_doubled() {
        let store = TencentCosBlobStore::new(CosConfig {
            bucket: "anystore-1250000000".into(),
            endpoint: "https://anystore-1250000000.light-cos.com/".into(),
            region: None,
            secret_id: "id".into(),
            secret_key: "key".into(),
            session_token: None,
        })
        .unwrap();
        assert_eq!(store.host, "anystore-1250000000.light-cos.com");
    }

    #[test]
    fn blob_keys_do_not_encode_object_names() {
        assert_eq!(store().blob_ref_for("upload_01").as_str(), "blobs/upload_01");
    }

    #[test]
    fn presigned_urls_carry_the_full_signature() {
        let url = store().presign("PUT", "/blobs/upload_01", &[], Duration::from_secs(600));
        assert!(url.starts_with("https://anystore-1250000000.light-cos.com/blobs/upload_01?"));
        for expected in [
            "q-sign-algorithm=sha1",
            "q-ak=AKIDEXAMPLE",
            "q-sign-time=",
            "q-key-time=",
            "q-header-list=host",
            "q-signature=",
        ] {
            assert!(url.contains(expected), "{url} is missing {expected}");
        }
        assert!(!url.contains("SECRETEXAMPLE"), "secret key leaked into URL");
    }

    #[test]
    fn part_urls_sign_the_multipart_parameters() {
        let url = store().presign(
            "PUT",
            "/blobs/upload_01",
            &[
                ("partNumber".into(), "2".into()),
                ("uploadId".into(), "abc".into()),
            ],
            Duration::from_secs(600),
        );
        assert!(url.contains("partNumber=2"));
        assert!(url.contains("uploadId=abc"));
        assert!(url.contains("q-url-param-list=partnumber;uploadid"));
    }

    #[test]
    fn download_urls_carry_the_current_object_name() {
        let url = store().sign_download_url_for_test("报告 2026.pdf");
        assert!(url.contains("response-content-disposition="));
        assert!(url.contains("q-url-param-list=response-content-disposition"));
    }

    #[test]
    fn upload_ids_are_extracted_from_the_initiate_response() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
  <Bucket>anystore-1250000000</Bucket>
  <Key>blobs/upload_01</Key>
  <UploadId>15281234567890abcdef</UploadId>
</InitiateMultipartUploadResult>"#;
        assert_eq!(
            extract_tag(xml, "UploadId").as_deref(),
            Some("15281234567890abcdef")
        );
    }

    #[test]
    fn error_codes_are_extracted() {
        let xml = br#"<Error><Code>NoSuchUpload</Code><Message>gone</Message></Error>"#;
        assert_eq!(extract_tag(xml, "Code").as_deref(), Some("NoSuchUpload"));
    }

    #[test]
    fn completion_body_is_sorted_and_escaped() {
        let body = complete_multipart_body(&[
            CompletedPart {
                part_number: 2,
                etag: "\"b\"".into(),
            },
            CompletedPart {
                part_number: 1,
                etag: "a&b".into(),
            },
        ]);
        let text = String::from_utf8(body).unwrap();
        assert!(text.find("<PartNumber>1<").unwrap() < text.find("<PartNumber>2<").unwrap());
        assert!(text.contains("a&amp;b"));
        assert!(text.contains("&quot;b&quot;"));
    }

    #[test]
    fn signatures_are_redacted_from_messages() {
        let message = "error for https://host/x?q-signature=abcdef123";
        assert_eq!(
            redact(message),
            "error for https://host/x?q-signature=<redacted>"
        );
    }

    #[test]
    fn debug_output_never_contains_credentials() {
        let rendered = format!("{:?}", store());
        assert!(!rendered.contains("SECRETEXAMPLE"));
        assert!(!rendered.contains("AKIDEXAMPLE"));
    }

    impl TencentCosBlobStore {
        fn sign_download_url_for_test(&self, filename: &str) -> String {
            let disposition = format!(
                "attachment; filename*=UTF-8''{}",
                signing::url_encode(filename)
            );
            self.presign(
                "GET",
                "/blobs/upload_01",
                &[("response-content-disposition".to_owned(), disposition)],
                Duration::from_secs(600),
            )
        }
    }
}
