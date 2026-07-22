use std::{
    convert::{TryFrom, TryInto},
    fmt,
    sync::{mpsc, Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use keyring_core::{CredentialStore as PlatformCredentialStore, Entry};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

const CREDENTIAL_RECORD_VERSION: u8 = 1;
const CREDENTIAL_SERVICE: &str = "lunatic-runtime-cli";
const CREDENTIAL_RECORD_MAGIC: &[u8] = b"LUNCLI\0";
const CREDENTIAL_TEXT_PREFIX: &[u8] = b"LUNATIC-CLI-CREDENTIAL-V1:";
const MAX_CREDENTIAL_LIFETIME_SECONDS: u64 = 30 * 24 * 60 * 60;
const MAX_COOKIE_COUNT: usize = 64;
// Windows Credential Manager limits generic credential blobs to 2,560 bytes.
// Keep the portable UTF-8 envelope below that limit and fail before any backend call.
const MAX_PROTECTED_SECRET_BYTES: usize = 2_400;

#[derive(Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct CredentialReference(String);

impl CredentialReference {
    pub(super) fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub(super) fn for_legacy_migration(cli_app_id: &str, provider: &str) -> Self {
        let locator = format!("lunatic-cli-legacy:{cli_app_id}:{provider}");
        Self(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, locator.as_bytes()).to_string())
    }

    fn as_str(&self) -> &str {
        &self.0
    }

    pub(super) fn validate(&self) -> Result<(), CredentialStoreError> {
        uuid::Uuid::parse_str(&self.0)
            .map(|_| ())
            .map_err(|_| CredentialStoreError::Invalid)
    }
}

impl fmt::Debug for CredentialReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialReference([REDACTED])")
    }
}

struct StoredCookie {
    value: String,
    expires_at_unix_seconds: Option<u64>,
}

impl Drop for StoredCookie {
    fn drop(&mut self) {
        self.value.zeroize();
    }
}

impl fmt::Debug for StoredCookie {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoredCookie([REDACTED])")
    }
}

pub(super) struct ControlCredential {
    version: u8,
    provider_url: String,
    cli_app_id: String,
    login_id: String,
    cookies: Vec<StoredCookie>,
}

impl ControlCredential {
    pub(super) fn from_set_cookie_headers(
        provider_url: &url::Url,
        cli_app_id: &str,
        login_id: String,
        headers: Vec<String>,
        now_unix_seconds: u64,
    ) -> Result<Self, CredentialStoreError> {
        let mut login_id = Zeroizing::new(login_id);
        if cli_app_id.is_empty() || login_id.is_empty() || headers.is_empty() {
            return Err(CredentialStoreError::Invalid);
        }
        let mut headers = Zeroizing::new(headers);
        if headers.len() > MAX_COOKIE_COUNT {
            return Err(CredentialStoreError::Invalid);
        }

        let mut cookies = Vec::with_capacity(headers.len());
        for header in headers.iter_mut() {
            cookies.push(StoredCookie::parse(
                std::mem::take(header),
                now_unix_seconds,
            )?);
        }

        let credential = Self {
            version: CREDENTIAL_RECORD_VERSION,
            provider_url: provider_url.as_str().to_owned(),
            cli_app_id: cli_app_id.to_owned(),
            login_id: std::mem::take(&mut *login_id),
            cookies,
        };
        credential.validate()?;
        Ok(credential)
    }

    pub(super) fn login_id(&self) -> &str {
        &self.login_id
    }

    pub(super) fn validate_scope(
        &self,
        provider_url: &url::Url,
        cli_app_id: &str,
    ) -> Result<(), CredentialStoreError> {
        self.validate()?;
        if self.provider_url == provider_url.as_str() && self.cli_app_id == cli_app_id {
            Ok(())
        } else {
            Err(CredentialStoreError::Invalid)
        }
    }

    pub(super) fn cookie_header_at(
        &self,
        now_unix_seconds: u64,
    ) -> Result<String, CredentialStoreError> {
        self.validate()?;
        if self.cookies.iter().any(|cookie| {
            cookie
                .expires_at_unix_seconds
                .map(|expires_at| expires_at <= now_unix_seconds)
                .unwrap_or(false)
        }) {
            return Err(CredentialStoreError::Expired);
        }
        Ok(self
            .cookies
            .iter()
            .map(|cookie| cookie.value.as_str())
            .collect::<Vec<_>>()
            .join("; "))
    }

    fn validate(&self) -> Result<(), CredentialStoreError> {
        if self.version != CREDENTIAL_RECORD_VERSION
            || self.provider_url.is_empty()
            || url::Url::parse(&self.provider_url)
                .map(|url| url.as_str() != self.provider_url)
                .unwrap_or(true)
            || self.cli_app_id.is_empty()
            || self.login_id.is_empty()
            || self.cookies.is_empty()
            || self.cookies.len() > MAX_COOKIE_COUNT
            || self.cookies.iter().any(|cookie| {
                cookie
                    .value
                    .split_once('=')
                    .map(|(name, _)| name.is_empty())
                    .unwrap_or(true)
            })
        {
            return Err(CredentialStoreError::Invalid);
        }
        Ok(())
    }

    pub(super) fn encode(&self) -> Result<Zeroizing<Vec<u8>>, CredentialStoreError> {
        self.validate()?;
        let mut record = Zeroizing::new(Vec::new());
        record.extend_from_slice(CREDENTIAL_RECORD_MAGIC);
        record.push(self.version);
        push_bytes(&mut record, self.provider_url.as_bytes())?;
        push_bytes(&mut record, self.cli_app_id.as_bytes())?;
        push_bytes(&mut record, self.login_id.as_bytes())?;
        push_u32(&mut record, self.cookies.len())?;
        for cookie in &self.cookies {
            push_bytes(&mut record, cookie.value.as_bytes())?;
            match cookie.expires_at_unix_seconds {
                Some(expires_at) => {
                    record.push(1);
                    record.extend_from_slice(&expires_at.to_be_bytes());
                }
                None => record.push(0),
            }
        }
        let payload = Zeroizing::new(STANDARD_NO_PAD.encode(record.as_slice()));
        let mut encoded = Zeroizing::new(Vec::with_capacity(
            CREDENTIAL_TEXT_PREFIX.len() + payload.len(),
        ));
        encoded.extend_from_slice(CREDENTIAL_TEXT_PREFIX);
        encoded.extend_from_slice(payload.as_bytes());
        if encoded.len() > MAX_PROTECTED_SECRET_BYTES {
            return Err(CredentialStoreError::TooLarge);
        }
        Ok(encoded)
    }

    pub(super) fn decode(encoded: &[u8]) -> Result<Self, CredentialStoreError> {
        if encoded.len() > MAX_PROTECTED_SECRET_BYTES
            || !encoded.starts_with(CREDENTIAL_TEXT_PREFIX)
        {
            return Err(CredentialStoreError::Invalid);
        }
        let record = Zeroizing::new(
            STANDARD_NO_PAD
                .decode(&encoded[CREDENTIAL_TEXT_PREFIX.len()..])
                .map_err(|_| CredentialStoreError::Invalid)?,
        );
        if !record.starts_with(CREDENTIAL_RECORD_MAGIC) {
            return Err(CredentialStoreError::Invalid);
        }
        let mut remaining = &record[CREDENTIAL_RECORD_MAGIC.len()..];
        let version = take_u8(&mut remaining)?;
        let provider_url_bytes = Zeroizing::new(take_bytes(&mut remaining)?);
        let provider_url = std::str::from_utf8(&provider_url_bytes)
            .map(str::to_owned)
            .map_err(|_| CredentialStoreError::Invalid)?;
        let cli_app_id_bytes = Zeroizing::new(take_bytes(&mut remaining)?);
        let cli_app_id = std::str::from_utf8(&cli_app_id_bytes)
            .map(str::to_owned)
            .map_err(|_| CredentialStoreError::Invalid)?;
        let login_id_bytes = Zeroizing::new(take_bytes(&mut remaining)?);
        let login_id = std::str::from_utf8(&login_id_bytes)
            .map(str::to_owned)
            .map_err(|_| CredentialStoreError::Invalid)?;
        let cookie_count = take_u32(&mut remaining)? as usize;
        if cookie_count == 0 || cookie_count > MAX_COOKIE_COUNT {
            return Err(CredentialStoreError::Invalid);
        }
        let mut cookies = Vec::with_capacity(cookie_count);
        for _ in 0..cookie_count {
            let value_bytes = Zeroizing::new(take_bytes(&mut remaining)?);
            let value = std::str::from_utf8(&value_bytes)
                .map(str::to_owned)
                .map_err(|_| CredentialStoreError::Invalid)?;
            let expires_at_unix_seconds = match take_u8(&mut remaining)? {
                0 => None,
                1 => Some(take_u64(&mut remaining)?),
                _ => return Err(CredentialStoreError::Invalid),
            };
            cookies.push(StoredCookie {
                value,
                expires_at_unix_seconds,
            });
        }
        if !remaining.is_empty() {
            return Err(CredentialStoreError::Invalid);
        }
        let credential = Self {
            version,
            provider_url,
            cli_app_id,
            login_id,
            cookies,
        };
        credential.validate()?;
        Ok(credential)
    }
}

impl Drop for ControlCredential {
    fn drop(&mut self) {
        self.provider_url.zeroize();
        self.cli_app_id.zeroize();
        self.login_id.zeroize();
    }
}

impl fmt::Debug for ControlCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlCredential")
            .field("version", &self.version)
            .field("provider_url", &"[REDACTED]")
            .field("cli_app_id", &"[REDACTED]")
            .field("login_id", &"[REDACTED]")
            .field("cookies", &"[REDACTED]")
            .finish()
    }
}

impl StoredCookie {
    fn parse(header: String, now_unix_seconds: u64) -> Result<Self, CredentialStoreError> {
        let header = Zeroizing::new(header);
        if header.contains('\r') || header.contains('\n') {
            return Err(CredentialStoreError::Invalid);
        }

        let mut segments = header.split(';');
        let value = segments
            .next()
            .map(str::trim)
            .filter(|value| {
                value
                    .split_once('=')
                    .map(|(name, _)| !name.trim().is_empty())
                    .unwrap_or(false)
            })
            .ok_or(CredentialStoreError::Invalid)?
            .to_owned();

        let local_expiry = now_unix_seconds.saturating_add(MAX_CREDENTIAL_LIFETIME_SECONDS);
        let mut expires_at_unix_seconds = Some(local_expiry);
        let mut max_age_seen = false;
        for segment in segments {
            let Some((name, raw_value)) = segment.trim().split_once('=') else {
                continue;
            };
            if name.eq_ignore_ascii_case("max-age") {
                let seconds = raw_value
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| CredentialStoreError::Invalid)?;
                max_age_seen = true;
                expires_at_unix_seconds = Some(if seconds <= 0 {
                    now_unix_seconds
                } else {
                    now_unix_seconds
                        .saturating_add(seconds as u64)
                        .min(local_expiry)
                });
            } else if !max_age_seen && name.eq_ignore_ascii_case("expires") {
                let expires_at = httpdate::parse_http_date(raw_value.trim())
                    .map_err(|_| CredentialStoreError::Invalid)?;
                expires_at_unix_seconds = Some(
                    expires_at
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| duration.as_secs())
                        .unwrap_or(0)
                        .min(local_expiry),
                );
            }
        }

        Ok(Self {
            value,
            expires_at_unix_seconds,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CredentialStoreError {
    Unavailable,
    Missing,
    Expired,
    Invalid,
    TooLarge,
    Failed,
}

impl fmt::Display for CredentialStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "CLI credential store is unavailable",
            Self::Missing => "CLI credential is unavailable",
            Self::Expired => "CLI credential has expired",
            Self::Invalid => "CLI credential is invalid",
            Self::TooLarge => "CLI credential exceeds the protected-store size limit",
            Self::Failed => "CLI credential store operation failed",
        })
    }
}

impl std::error::Error for CredentialStoreError {}

pub(super) trait CredentialStore: Send + Sync {
    fn put(
        &self,
        reference: &CredentialReference,
        credential: &ControlCredential,
    ) -> Result<(), CredentialStoreError>;

    fn get(
        &self,
        reference: &CredentialReference,
    ) -> Result<ControlCredential, CredentialStoreError>;

    fn delete(&self, reference: &CredentialReference) -> Result<(), CredentialStoreError>;
}

pub(super) struct OsCredentialStore {
    worker: OnceLock<Result<PlatformWorker, CredentialStoreError>>,
}

impl OsCredentialStore {
    pub(super) fn new() -> Self {
        Self {
            worker: OnceLock::new(),
        }
    }

    fn worker(&self) -> Result<&PlatformWorker, CredentialStoreError> {
        match self.worker.get_or_init(spawn_platform_worker) {
            Ok(worker) => Ok(worker),
            Err(error) => Err(*error),
        }
    }
}

impl CredentialStore for OsCredentialStore {
    fn put(
        &self,
        reference: &CredentialReference,
        credential: &ControlCredential,
    ) -> Result<(), CredentialStoreError> {
        reference.validate()?;
        credential.validate()?;
        let encoded = credential.encode()?;
        self.worker()?.put(reference.clone(), encoded)
    }

    fn get(
        &self,
        reference: &CredentialReference,
    ) -> Result<ControlCredential, CredentialStoreError> {
        reference.validate()?;
        let encoded = self.worker()?.get(reference.clone())?;
        ControlCredential::decode(&encoded)
    }

    fn delete(&self, reference: &CredentialReference) -> Result<(), CredentialStoreError> {
        reference.validate()?;
        self.worker()?.delete(reference.clone())
    }
}

struct PlatformWorker {
    requests: Option<mpsc::Sender<PlatformRequest>>,
}

impl PlatformWorker {
    fn requests(&self) -> Result<&mpsc::Sender<PlatformRequest>, CredentialStoreError> {
        self.requests.as_ref().ok_or(CredentialStoreError::Failed)
    }

    fn put(
        &self,
        reference: CredentialReference,
        encoded: Zeroizing<Vec<u8>>,
    ) -> Result<(), CredentialStoreError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests()?
            .send(PlatformRequest::Put {
                reference,
                encoded,
                reply,
            })
            .map_err(|_| CredentialStoreError::Failed)?;
        response.recv().map_err(|_| CredentialStoreError::Failed)?
    }

    fn get(
        &self,
        reference: CredentialReference,
    ) -> Result<Zeroizing<Vec<u8>>, CredentialStoreError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests()?
            .send(PlatformRequest::Get { reference, reply })
            .map_err(|_| CredentialStoreError::Failed)?;
        response.recv().map_err(|_| CredentialStoreError::Failed)?
    }

    fn delete(&self, reference: CredentialReference) -> Result<(), CredentialStoreError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests()?
            .send(PlatformRequest::Delete { reference, reply })
            .map_err(|_| CredentialStoreError::Failed)?;
        response.recv().map_err(|_| CredentialStoreError::Failed)?
    }

    #[cfg(test)]
    fn thread_id(&self) -> Result<std::thread::ThreadId, CredentialStoreError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.requests()?
            .send(PlatformRequest::ThreadId { reply })
            .map_err(|_| CredentialStoreError::Failed)?;
        response.recv().map_err(|_| CredentialStoreError::Failed)
    }
}

impl Drop for PlatformWorker {
    fn drop(&mut self) {
        // Closing the request channel lets the detached worker exit and drop the
        // native store on its own thread. Waiting here could deadlock on a stuck
        // platform service, so shutdown deliberately never joins.
        let _ = self.requests.take();
    }
}

enum PlatformRequest {
    Put {
        reference: CredentialReference,
        encoded: Zeroizing<Vec<u8>>,
        reply: mpsc::SyncSender<Result<(), CredentialStoreError>>,
    },
    Get {
        reference: CredentialReference,
        reply: mpsc::SyncSender<Result<Zeroizing<Vec<u8>>, CredentialStoreError>>,
    },
    Delete {
        reference: CredentialReference,
        reply: mpsc::SyncSender<Result<(), CredentialStoreError>>,
    },
    #[cfg(test)]
    ThreadId {
        reply: mpsc::SyncSender<std::thread::ThreadId>,
    },
}

struct PlatformWorkerState {
    store: Option<Result<Arc<PlatformCredentialStore>, CredentialStoreError>>,
}

impl PlatformWorkerState {
    fn new() -> Self {
        Self { store: None }
    }

    fn store(&mut self) -> Result<&Arc<PlatformCredentialStore>, CredentialStoreError> {
        self.store
            .get_or_insert_with(initialize_platform_store)
            .as_ref()
            .map_err(|error| *error)
    }

    fn entry(&mut self, reference: &CredentialReference) -> Result<Entry, CredentialStoreError> {
        reference.validate()?;
        #[cfg(target_os = "windows")]
        {
            let modifiers = std::collections::HashMap::from([("persistence", "Local")]);
            return self
                .store()?
                .build(CREDENTIAL_SERVICE, reference.as_str(), Some(&modifiers))
                .map_err(map_platform_error);
        }
        #[cfg(not(target_os = "windows"))]
        self.store()?
            .build(CREDENTIAL_SERVICE, reference.as_str(), None)
            .map_err(map_platform_error)
    }

    fn put(
        &mut self,
        reference: &CredentialReference,
        encoded: &[u8],
    ) -> Result<(), CredentialStoreError> {
        self.entry(reference)?
            .set_secret(encoded)
            .map_err(map_platform_error)
    }

    fn get(
        &mut self,
        reference: &CredentialReference,
    ) -> Result<Zeroizing<Vec<u8>>, CredentialStoreError> {
        self.entry(reference)?
            .get_secret()
            .map(Zeroizing::new)
            .map_err(map_platform_error)
    }

    fn delete(&mut self, reference: &CredentialReference) -> Result<(), CredentialStoreError> {
        let entry = self.entry(reference)?;
        entry.delete_credential().map_err(map_platform_error)?;
        match entry.get_secret() {
            Err(keyring_core::Error::NoEntry) => Ok(()),
            Ok(remaining) => {
                let _remaining = Zeroizing::new(remaining);
                Err(CredentialStoreError::Failed)
            }
            Err(error) => Err(map_platform_error(error)),
        }
    }
}

fn spawn_platform_worker() -> Result<PlatformWorker, CredentialStoreError> {
    let (requests, receiver) = mpsc::channel();
    let handle = std::thread::Builder::new()
        .name("lunatic-credential-store".to_owned())
        .spawn(move || platform_worker_loop(receiver))
        .map_err(|_| CredentialStoreError::Unavailable)?;
    drop(handle);
    Ok(PlatformWorker {
        requests: Some(requests),
    })
}

fn platform_worker_loop(receiver: mpsc::Receiver<PlatformRequest>) {
    let mut state = PlatformWorkerState::new();
    while let Ok(request) = receiver.recv() {
        match request {
            PlatformRequest::Put {
                reference,
                encoded,
                reply,
            } => {
                let _ = reply.send(state.put(&reference, &encoded));
            }
            PlatformRequest::Get { reference, reply } => {
                let _ = reply.send(state.get(&reference));
            }
            PlatformRequest::Delete { reference, reply } => {
                let _ = reply.send(state.delete(&reference));
            }
            #[cfg(test)]
            PlatformRequest::ThreadId { reply } => {
                let _ = reply.send(std::thread::current().id());
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn initialize_platform_store() -> Result<Arc<PlatformCredentialStore>, CredentialStoreError> {
    apple_native_keyring_store::keychain::Store::new()
        .map(|store| store as Arc<PlatformCredentialStore>)
        .map_err(map_platform_initialization_error)
}

#[cfg(target_os = "windows")]
fn initialize_platform_store() -> Result<Arc<PlatformCredentialStore>, CredentialStoreError> {
    windows_native_keyring_store::Store::new()
        .map(|store| store as Arc<PlatformCredentialStore>)
        .map_err(map_platform_initialization_error)
}

#[cfg(target_os = "linux")]
fn initialize_platform_store() -> Result<Arc<PlatformCredentialStore>, CredentialStoreError> {
    zbus_secret_service_keyring_store::Store::new()
        .map(|store| store as Arc<PlatformCredentialStore>)
        .map_err(map_platform_initialization_error)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn initialize_platform_store() -> Result<Arc<PlatformCredentialStore>, CredentialStoreError> {
    Err(CredentialStoreError::Unavailable)
}

fn map_platform_error(error: keyring_core::Error) -> CredentialStoreError {
    match error {
        keyring_core::Error::NoEntry => CredentialStoreError::Missing,
        keyring_core::Error::NoDefaultStore
        | keyring_core::Error::NoStorageAccess(_)
        | keyring_core::Error::NotSupportedByStore(_) => CredentialStoreError::Unavailable,
        keyring_core::Error::BadEncoding(mut bytes) => {
            bytes.zeroize();
            CredentialStoreError::Invalid
        }
        keyring_core::Error::BadDataFormat(mut bytes, _) => {
            bytes.zeroize();
            CredentialStoreError::Invalid
        }
        keyring_core::Error::BadStoreFormat(_) | keyring_core::Error::Invalid(_, _) => {
            CredentialStoreError::Invalid
        }
        keyring_core::Error::TooLong(_, _) => CredentialStoreError::TooLarge,
        _ => CredentialStoreError::Failed,
    }
}

fn map_platform_initialization_error(error: keyring_core::Error) -> CredentialStoreError {
    match error {
        keyring_core::Error::PlatformFailure(_) => CredentialStoreError::Unavailable,
        error => map_platform_error(error),
    }
}

fn push_u32(encoded: &mut Vec<u8>, value: usize) -> Result<(), CredentialStoreError> {
    let value = u32::try_from(value).map_err(|_| CredentialStoreError::TooLarge)?;
    encoded.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn push_bytes(encoded: &mut Vec<u8>, value: &[u8]) -> Result<(), CredentialStoreError> {
    push_u32(encoded, value.len())?;
    encoded.extend_from_slice(value);
    Ok(())
}

fn take_u8(encoded: &mut &[u8]) -> Result<u8, CredentialStoreError> {
    let (value, remaining) = encoded.split_first().ok_or(CredentialStoreError::Invalid)?;
    *encoded = remaining;
    Ok(*value)
}

fn take_u32(encoded: &mut &[u8]) -> Result<u32, CredentialStoreError> {
    let bytes = take_exact(encoded, 4)?;
    Ok(u32::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| CredentialStoreError::Invalid)?,
    ))
}

fn take_u64(encoded: &mut &[u8]) -> Result<u64, CredentialStoreError> {
    let bytes = take_exact(encoded, 8)?;
    Ok(u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| CredentialStoreError::Invalid)?,
    ))
}

fn take_bytes(encoded: &mut &[u8]) -> Result<Vec<u8>, CredentialStoreError> {
    let length = take_u32(encoded)? as usize;
    Ok(take_exact(encoded, length)?.to_vec())
}

fn take_exact<'a>(encoded: &mut &'a [u8], length: usize) -> Result<&'a [u8], CredentialStoreError> {
    if encoded.len() < length {
        return Err(CredentialStoreError::Invalid);
    }
    let (value, remaining) = encoded.split_at(length);
    *encoded = remaining;
    Ok(value)
}

pub(super) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider_url() -> url::Url {
        url::Url::parse("https://example.invalid/").unwrap()
    }

    fn test_credential(
        login_id: &str,
        cookies: Vec<String>,
        now_unix_seconds: u64,
    ) -> ControlCredential {
        ControlCredential::from_set_cookie_headers(
            &provider_url(),
            "app-id",
            login_id.to_owned(),
            cookies,
            now_unix_seconds,
        )
        .unwrap()
    }

    #[test]
    fn cookie_headers_are_sanitized_and_expire_locally() {
        let credential = test_credential(
            "login",
            vec!["session=marker; HttpOnly; Max-Age=5".to_owned()],
            100,
        );

        assert_eq!(credential.cookie_header_at(104).unwrap(), "session=marker");
        assert_eq!(
            credential.cookie_header_at(105).unwrap_err(),
            CredentialStoreError::Expired
        );
    }

    #[test]
    fn one_expired_cookie_rejects_the_whole_credential() {
        let credential = test_credential(
            "login",
            vec![
                "session=marker; Max-Age=30".to_owned(),
                "csrf=marker; Max-Age=0".to_owned(),
            ],
            100,
        );
        assert_eq!(
            credential.cookie_header_at(100).unwrap_err(),
            CredentialStoreError::Expired
        );
    }

    #[test]
    fn max_age_takes_precedence_over_expires() {
        let credential = test_credential(
            "login",
            vec!["session=marker; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=5".to_owned()],
            100,
        );

        assert_eq!(credential.cookie_header_at(104).unwrap(), "session=marker");
        assert_eq!(
            credential.cookie_header_at(105).unwrap_err(),
            CredentialStoreError::Expired
        );
    }

    #[test]
    fn session_cookie_has_a_bounded_local_lifetime() {
        let credential = test_credential("login", vec!["session=marker".to_owned()], 100);
        assert_eq!(
            credential
                .cookie_header_at(100 + MAX_CREDENTIAL_LIFETIME_SECONDS - 1)
                .unwrap(),
            "session=marker"
        );
        assert_eq!(
            credential
                .cookie_header_at(100 + MAX_CREDENTIAL_LIFETIME_SECONDS)
                .unwrap_err(),
            CredentialStoreError::Expired
        );
    }

    #[test]
    fn protected_record_enforces_the_cross_platform_size_ceiling() {
        let mut largest = None;
        let mut first_failure = None;
        for payload_len in 0..5_000 {
            let credential =
                test_credential("login", vec![format!("s={}", "x".repeat(payload_len))], 100);
            match credential.encode() {
                Ok(encoded) => largest = Some((payload_len, encoded)),
                Err(CredentialStoreError::TooLarge) => {
                    first_failure = Some(payload_len);
                    break;
                }
                Err(error) => panic!("unexpected credential encoding error: {}", error),
            }
        }

        let (largest_payload, encoded) = largest.unwrap();
        assert_eq!(first_failure, Some(largest_payload + 1));
        assert!(encoded.len() <= MAX_PROTECTED_SECRET_BYTES);
        assert!(MAX_PROTECTED_SECRET_BYTES - encoded.len() < 4);
        assert!(std::str::from_utf8(&encoded).is_ok());
        assert_eq!(
            ControlCredential::decode(&encoded).unwrap().login_id(),
            "login"
        );
    }

    #[test]
    fn maximum_cookie_count_round_trips_at_the_exact_boundary() {
        let headers = (0..MAX_COOKIE_COUNT)
            .map(|index| format!("c{index}=v"))
            .collect::<Vec<_>>();
        let expected_header = headers.join("; ");
        let credential = test_credential("login", headers, 100);

        assert_eq!(credential.cookies.len(), MAX_COOKIE_COUNT);
        let decoded = ControlCredential::decode(&credential.encode().unwrap()).unwrap();
        assert_eq!(decoded.cookies.len(), MAX_COOKIE_COUNT);
        assert_eq!(decoded.cookie_header_at(100).unwrap(), expected_header);
    }

    #[test]
    fn cookie_count_above_the_limit_is_rejected_before_encoding() {
        let headers = (0..=MAX_COOKIE_COUNT)
            .map(|index| format!("cookie{index}=value{index}"))
            .collect::<Vec<_>>();

        assert_eq!(
            ControlCredential::from_set_cookie_headers(
                &provider_url(),
                "app-id",
                "login".to_owned(),
                headers,
                100,
            )
            .unwrap_err(),
            CredentialStoreError::Invalid
        );
    }

    #[test]
    fn platform_error_bytes_are_discarded() {
        let marker = b"cookie-marker".to_vec();
        let error = map_platform_error(keyring_core::Error::BadEncoding(marker));
        assert_eq!(error, CredentialStoreError::Invalid);
        assert!(!error.to_string().contains("cookie-marker"));
    }

    #[test]
    fn platform_initialization_failure_maps_to_unavailable_without_detail() {
        let error =
            map_platform_initialization_error(keyring_core::Error::PlatformFailure(Box::new(
                std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "dbus-secret-marker"),
            )));
        assert_eq!(error, CredentialStoreError::Unavailable);
        assert!(!error.to_string().contains("dbus-secret-marker"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn platform_calls_share_one_worker_outside_the_runtime() {
        let store = OsCredentialStore::new();
        let runtime_thread = std::thread::current().id();
        let first_platform_thread = store.worker().unwrap().thread_id().unwrap();
        let second_platform_thread = store.worker().unwrap().thread_id().unwrap();

        assert_ne!(runtime_thread, first_platform_thread);
        assert_eq!(first_platform_thread, second_platform_thread);
    }

    #[test]
    fn debug_and_errors_do_not_expose_markers() {
        let credential = test_credential(
            "login-marker",
            vec!["session=cookie-marker".to_owned()],
            100,
        );
        let debug = format!("{credential:?}");
        let encoded = credential.encode().unwrap();
        assert!(!debug.contains("login-marker"));
        assert!(!debug.contains("cookie-marker"));
        assert!(!encoded
            .windows("cookie-marker".len())
            .any(|window| window == b"cookie-marker"));
        assert_eq!(
            CredentialStoreError::Failed.to_string(),
            "CLI credential store operation failed"
        );
    }

    #[test]
    fn malformed_cookie_fails_without_echoing_input() {
        let marker = "secret-marker";
        let error = ControlCredential::from_set_cookie_headers(
            &provider_url(),
            "app-id",
            "login".to_owned(),
            vec![format!("session={marker}\r\nInjected: true")],
            100,
        )
        .unwrap_err();
        assert_eq!(error, CredentialStoreError::Invalid);
        assert!(!error.to_string().contains(marker));
    }

    #[test]
    fn credential_scope_rejects_provider_or_app_id_changes() {
        let credential = test_credential("login", vec!["session=marker".to_owned()], 100);
        assert_eq!(
            credential
                .validate_scope(
                    &url::Url::parse("https://attacker.invalid/").unwrap(),
                    "app-id"
                )
                .unwrap_err(),
            CredentialStoreError::Invalid
        );
        assert_eq!(
            credential
                .validate_scope(&provider_url(), "other-app")
                .unwrap_err(),
            CredentialStoreError::Invalid
        );
    }
}
