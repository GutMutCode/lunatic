use std::{
    collections::HashMap,
    error::Error,
    fmt,
    sync::Mutex,
    time::{Duration, Instant},
};

use lunatic_process::resource_migration::TlsCredentialHandle;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

pub const DEFAULT_TLS_CREDENTIAL_TTL: Duration = Duration::from_secs(5 * 60);
pub const DEFAULT_TLS_CREDENTIAL_CAPACITY: usize = 1024;

/// Runtime scope that authorizes access to a provisioned TLS credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TlsCredentialScope {
    environment_id: u64,
    process_id: u64,
}

impl TlsCredentialScope {
    pub fn new(environment_id: u64, process_id: u64) -> Self {
        Self {
            environment_id,
            process_id,
        }
    }

    pub fn environment_id(self) -> u64 {
        self.environment_id
    }

    pub fn process_id(self) -> u64 {
        self.process_id
    }
}

/// Host-owned TLS listener credential material.
///
/// This type is deliberately not serializable. Providers may retain it in a
/// process-local store, HSM adapter, or another secure re-provisioning system.
pub struct TlsCredentialMaterial {
    acceptor: TlsAcceptor,
}

impl TlsCredentialMaterial {
    pub fn new(acceptor: TlsAcceptor) -> Self {
        Self { acceptor }
    }

    pub fn into_acceptor(self) -> TlsAcceptor {
        self.acceptor
    }
}

impl Clone for TlsCredentialMaterial {
    fn clone(&self) -> Self {
        Self {
            acceptor: self.acceptor.clone(),
        }
    }
}

impl fmt::Debug for TlsCredentialMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsCredentialMaterial")
            .field("acceptor", &"[REDACTED]")
            .finish()
    }
}

/// Stable, secret-free failures returned by a TLS credential provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsCredentialProviderError {
    Unavailable,
    Expired,
    ProviderFailure,
}

impl fmt::Display for TlsCredentialProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("TLS listener credential is unavailable"),
            Self::Expired => formatter.write_str("TLS listener credential has expired"),
            Self::ProviderFailure => formatter.write_str("TLS credential provider failed"),
        }
    }
}

impl Error for TlsCredentialProviderError {}

/// Secure boundary used to provision and consume TLS listener credentials.
///
/// Handles are single-use: a successful `take` removes the credential from the
/// provider. Implementations must not include handle values or secret material
/// in returned errors.
pub trait TlsCredentialProvider: Send + Sync {
    fn provision(
        &self,
        scope: TlsCredentialScope,
        material: TlsCredentialMaterial,
    ) -> Result<TlsCredentialHandle, TlsCredentialProviderError>;

    fn take(
        &self,
        scope: TlsCredentialScope,
        handle: &TlsCredentialHandle,
    ) -> Result<TlsCredentialMaterial, TlsCredentialProviderError>;
}

struct StoredCredential {
    scope: TlsCredentialScope,
    material: TlsCredentialMaterial,
    expires_at: Instant,
}

/// Default process-local credential provider.
///
/// It issues random, single-use handles and denies access after a bounded TTL.
/// Persisted snapshots require an explicitly injected provider capable of
/// resolving the same handles after restart.
pub struct EphemeralTlsCredentialProvider {
    ttl: Duration,
    capacity: usize,
    credentials: Mutex<HashMap<TlsCredentialHandle, StoredCredential>>,
}

impl EphemeralTlsCredentialProvider {
    pub fn new(ttl: Duration) -> Self {
        Self::with_capacity(ttl, DEFAULT_TLS_CREDENTIAL_CAPACITY)
    }

    pub fn with_capacity(ttl: Duration, capacity: usize) -> Self {
        Self {
            ttl,
            capacity,
            credentials: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for EphemeralTlsCredentialProvider {
    fn default() -> Self {
        Self::new(DEFAULT_TLS_CREDENTIAL_TTL)
    }
}

impl fmt::Debug for EphemeralTlsCredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EphemeralTlsCredentialProvider")
            .field("ttl", &self.ttl)
            .field("capacity", &self.capacity)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

impl TlsCredentialProvider for EphemeralTlsCredentialProvider {
    fn provision(
        &self,
        scope: TlsCredentialScope,
        material: TlsCredentialMaterial,
    ) -> Result<TlsCredentialHandle, TlsCredentialProviderError> {
        let now = Instant::now();
        let expires_at = now
            .checked_add(self.ttl)
            .ok_or(TlsCredentialProviderError::ProviderFailure)?;
        let mut credentials = self
            .credentials
            .lock()
            .map_err(|_| TlsCredentialProviderError::ProviderFailure)?;
        credentials.retain(|_, stored| stored.expires_at > now);
        if credentials.len() >= self.capacity {
            return Err(TlsCredentialProviderError::ProviderFailure);
        }

        let handle = loop {
            let candidate = TlsCredentialHandle::from_bytes(*Uuid::new_v4().as_bytes());
            if !credentials.contains_key(&candidate) {
                break candidate;
            }
        };
        credentials.insert(
            handle,
            StoredCredential {
                scope,
                material,
                expires_at,
            },
        );
        Ok(handle)
    }

    fn take(
        &self,
        scope: TlsCredentialScope,
        handle: &TlsCredentialHandle,
    ) -> Result<TlsCredentialMaterial, TlsCredentialProviderError> {
        let mut credentials = self
            .credentials
            .lock()
            .map_err(|_| TlsCredentialProviderError::ProviderFailure)?;
        let Some(stored) = credentials.get(handle) else {
            return Err(TlsCredentialProviderError::Unavailable);
        };
        if stored.scope != scope {
            return Err(TlsCredentialProviderError::Unavailable);
        }
        if Instant::now() >= stored.expires_at {
            credentials.remove(handle);
            return Err(TlsCredentialProviderError::Expired);
        }
        let stored = credentials
            .remove(handle)
            .ok_or(TlsCredentialProviderError::ProviderFailure)?;
        Ok(stored.material)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio_rustls::rustls::{
        server::{ClientHello, ResolvesServerCert},
        ServerConfig,
    };

    #[derive(Debug)]
    struct EmptyCertificateResolver;

    impl ResolvesServerCert for EmptyCertificateResolver {
        fn resolve(
            &self,
            _client_hello: ClientHello<'_>,
        ) -> Option<Arc<tokio_rustls::rustls::sign::CertifiedKey>> {
            None
        }
    }

    fn material() -> TlsCredentialMaterial {
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(EmptyCertificateResolver));
        TlsCredentialMaterial::new(TlsAcceptor::from(Arc::new(config)))
    }

    fn scope(process_id: u64) -> TlsCredentialScope {
        TlsCredentialScope::new(11, process_id)
    }

    #[test]
    fn credential_material_debug_is_redacted() {
        let material = material();
        let debug = format!("{material:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("EmptyCertificateResolver"));
    }

    #[test]
    fn ephemeral_handles_are_single_use_and_missing_is_stable() {
        let provider = EphemeralTlsCredentialProvider::default();
        let handle = provider.provision(scope(7), material()).unwrap();

        provider.take(scope(7), &handle).unwrap();
        let error = provider.take(scope(7), &handle).unwrap_err();
        assert_eq!(error, TlsCredentialProviderError::Unavailable);
        assert_eq!(error.to_string(), "TLS listener credential is unavailable");
    }

    #[test]
    fn handle_is_not_authority_outside_its_scope() {
        let provider = EphemeralTlsCredentialProvider::default();
        let handle = provider.provision(scope(7), material()).unwrap();

        let error = provider.take(scope(8), &handle).unwrap_err();
        assert_eq!(error, TlsCredentialProviderError::Unavailable);
        provider.take(scope(7), &handle).unwrap();
    }

    #[test]
    fn expired_handles_fail_without_exposing_the_handle() {
        let provider = EphemeralTlsCredentialProvider::new(Duration::ZERO);
        let handle = provider.provision(scope(7), material()).unwrap();

        let error = provider.take(scope(7), &handle).unwrap_err();
        let message = error.to_string();
        assert_eq!(error, TlsCredentialProviderError::Expired);
        assert_eq!(message, "TLS listener credential has expired");
        assert!(!message.contains(&format!("{:?}", handle.as_bytes())));
    }

    #[test]
    fn credential_capacity_is_bounded_and_recovers_after_consumption() {
        let provider = EphemeralTlsCredentialProvider::with_capacity(Duration::from_secs(60), 1);
        let first = provider.provision(scope(7), material()).unwrap();

        let error = provider.provision(scope(7), material()).unwrap_err();
        assert_eq!(error, TlsCredentialProviderError::ProviderFailure);
        assert_eq!(error.to_string(), "TLS credential provider failed");

        provider.take(scope(7), &first).unwrap();
        provider.provision(scope(7), material()).unwrap();
    }
}
