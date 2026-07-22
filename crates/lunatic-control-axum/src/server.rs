use std::{
    collections::HashMap,
    fmt,
    net::{SocketAddr, TcpListener},
    sync::{
        atomic::{self, AtomicU64},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use axum::{Extension, Router};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use lunatic_control::api::{
    BearerToken, ControlUrls, NodeStart, Register, WireBearerToken, DEFAULT_NODE_BEARER_TTL_SECONDS,
};
use lunatic_distributed::{
    control::cert::{sign_node_certificate, CertificateAuthority},
    CertAttrs,
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;
use zeroize::Zeroize;

use crate::routes;

const EXPIRED_REGISTRATION_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct ControlServer {
    pub ca_cert: CertificateAuthority,
    pub quic_client: lunatic_distributed::quic::Client,
    pub registrations: DashMap<u64, Registered>,
    pub nodes: DashMap<u64, NodeDetails>,
    pub modules: DashMap<u64, Vec<u8>>,
    public_origin: String,
    token_ttl: Duration,
    node_lifecycle: Mutex<()>,
    next_registration_id: AtomicU64,
    next_node_id: AtomicU64,
    next_module_id: AtomicU64,
}

pub struct Registered {
    pub node_name: Uuid,
    pub csr_pem: String,
    pub cert_pem: String,
    token: TokenVerifier,
}

impl fmt::Debug for Registered {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registered")
            .field("node_name", &self.node_name)
            .field("csr_pem", &"[REDACTED]")
            .field("cert_pem", &"[REDACTED]")
            .field("token", &"[REDACTED VERIFIER]")
            .finish()
    }
}

struct TokenVerifier {
    current: [u8; 32],
    pending: Option<[u8; 32]>,
    generation: u64,
    expires_at: Instant,
}

pub struct AuthenticatedRegistration {
    pub registration_id: u64,
    pub bearer_generation: u64,
}

impl TokenVerifier {
    fn new(current: [u8; 32], now: Instant, ttl: Duration) -> Self {
        Self {
            current,
            pending: None,
            generation: 0,
            expires_at: now + ttl,
        }
    }

    fn authenticate(&mut self, candidate: &[u8; 32], now: Instant, ttl: Duration) -> Option<u64> {
        if now > self.expires_at {
            return None;
        }

        let current_matches = bool::from(self.current.ct_eq(candidate));
        let pending_matches = self
            .pending
            .as_ref()
            .is_some_and(|pending| bool::from(pending.ct_eq(candidate)));
        if !(current_matches | pending_matches) {
            return None;
        }

        if pending_matches {
            let next_generation = self.generation.checked_add(1)?;
            self.current.zeroize();
            self.current = self.pending.take()?;
            self.generation = next_generation;
        }
        self.expires_at = now + ttl;
        Some(self.generation)
    }

    fn stage_rotation(&mut self, expected_generation: u64, next: [u8; 32]) -> Result<u64> {
        anyhow::ensure!(
            self.generation == expected_generation,
            "bearer generation changed"
        );
        anyhow::ensure!(
            !bool::from(self.current.ct_eq(&next)),
            "bearer rotation reused current credential"
        );
        if let Some(pending) = self.pending {
            anyhow::ensure!(
                bool::from(pending.ct_eq(&next)),
                "a different bearer rotation is already pending"
            );
        } else {
            self.pending = Some(next);
        }
        self.generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("bearer generation exhausted"))
    }

    fn is_expired(&self, now: Instant) -> bool {
        now > self.expires_at
    }
}

impl Drop for TokenVerifier {
    fn drop(&mut self) {
        self.current.zeroize();
        if let Some(pending) = &mut self.pending {
            pending.zeroize();
        }
    }
}

pub struct NodeDetails {
    pub registration_id: u64,
    pub status: i16,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub node_address: String,
    pub attributes: HashMap<String, String>,
}

impl ControlServer {
    pub(crate) fn new(
        ca_cert: CertificateAuthority,
        quic_client: lunatic_distributed::quic::Client,
        public_origin: String,
    ) -> Self {
        Self::new_with_token_ttl(
            ca_cert,
            quic_client,
            public_origin,
            Duration::from_secs(DEFAULT_NODE_BEARER_TTL_SECONDS),
        )
    }

    pub(crate) fn new_with_token_ttl(
        ca_cert: CertificateAuthority,
        quic_client: lunatic_distributed::quic::Client,
        public_origin: String,
        token_ttl: Duration,
    ) -> Self {
        Self {
            ca_cert,
            quic_client,
            registrations: DashMap::new(),
            nodes: DashMap::new(),
            modules: DashMap::new(),
            public_origin,
            token_ttl,
            node_lifecycle: Mutex::new(()),
            next_registration_id: AtomicU64::new(1),
            next_node_id: AtomicU64::new(1),
            next_module_id: AtomicU64::new(1),
        }
    }

    pub fn control_urls(&self) -> ControlUrls {
        let base = self.public_origin.trim_end_matches('/');
        ControlUrls {
            api_base: format!("{base}/"),
            nodes: format!("{base}/nodes"),
            node_started: format!("{base}/started"),
            node_refreshed: format!("{base}/refreshed"),
            node_stopped: format!("{base}/stopped"),
            get_module: format!("{base}/module/{{id}}"),
            add_module: format!("{base}/module"),
            get_nodes: format!("{base}/nodes"),
        }
    }

    pub fn bearer_ttl_seconds(&self) -> u64 {
        self.token_ttl.as_secs().max(1)
    }

    pub fn register(&self, reg: &Register, cert_pem: &str) -> Result<WireBearerToken> {
        let _lifecycle = self
            .node_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        self.purge_expired_locked(now);

        let duplicate = self
            .registrations
            .iter()
            .any(|registered| registered.node_name == reg.node_name);
        anyhow::ensure!(!duplicate, "node name is already registered");

        let (authentication_token, token_digest) = issue_bearer()?;
        let id = self
            .next_registration_id
            .fetch_add(1, atomic::Ordering::Relaxed);
        let registered = Registered {
            node_name: reg.node_name,
            csr_pem: reg.csr_pem.clone(),
            cert_pem: cert_pem.to_owned(),
            token: TokenVerifier::new(token_digest, now, self.token_ttl),
        };
        self.registrations.insert(id, registered);
        Ok(authentication_token)
    }

    pub fn start_node(
        &self,
        registration_id: u64,
        bearer_generation: u64,
        data: NodeStart,
    ) -> Result<(u64, String, String)> {
        let _lifecycle = self
            .node_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.purge_expired_locked(Instant::now());

        let (csr_pem, node_name) = {
            let registration = self
                .registrations
                .get(&registration_id)
                .ok_or_else(|| anyhow!("registration no longer exists"))?;
            anyhow::ensure!(
                registration.token.generation == bearer_generation,
                "bearer generation changed"
            );
            (
                registration.csr_pem.clone(),
                registration.node_name.hyphenated().to_string(),
            )
        };
        let id = self.next_node_id.load(atomic::Ordering::Relaxed);
        anyhow::ensure!(
            id <= lunatic_distributed::distributed::MAX_NODE_ID,
            "distributed node ID space is exhausted"
        );
        let cert_pem = sign_node_certificate(
            &csr_pem,
            &self.ca_cert,
            &node_name,
            &CertAttrs {
                node_id: Some(id),
                allowed_envs: vec![],
                is_privileged: true,
            },
        )?;

        // Certificate signing can itself cross the inactivity deadline. Check
        // the lease and generation again at the actual state-commit boundary.
        self.purge_expired_locked(Instant::now());
        {
            let registration = self
                .registrations
                .get(&registration_id)
                .ok_or_else(|| anyhow!("registration no longer exists"))?;
            anyhow::ensure!(
                registration.token.generation == bearer_generation,
                "bearer generation changed"
            );
        }

        // Keep the old node active if certificate issuance fails. Once the
        // replacement identity is ready, lifecycle and certificate state move
        // together while this lock excludes a concurrent restart/stop.
        self.next_node_id.store(id + 1, atomic::Ordering::Relaxed);
        self.stop_nodes_for_registration(registration_id);
        {
            let mut registration = self
                .registrations
                .get_mut(&registration_id)
                .ok_or_else(|| anyhow!("registration no longer exists"))?;
            registration.cert_pem = cert_pem.clone();
        }

        let details = NodeDetails {
            registration_id,
            status: 0,
            created_at: Utc::now(),
            stopped_at: None,
            node_address: data.node_address.to_string(),
            attributes: data.attributes,
        };
        self.nodes.insert(id, details);
        Ok((id, data.node_address.to_string(), cert_pem))
    }

    pub fn stage_bearer_rotation(
        &self,
        registration_id: u64,
        bearer_generation: u64,
        next: &WireBearerToken,
    ) -> Result<u64> {
        let next_digest = token_digest(next.expose_for_wire());
        let _lifecycle = self
            .node_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.purge_expired_locked(Instant::now());
        let mut registration = self
            .registrations
            .get_mut(&registration_id)
            .ok_or_else(|| anyhow!("registration no longer exists"))?;
        registration
            .token
            .stage_rotation(bearer_generation, next_digest)
    }

    pub fn authenticate(&self, node_name: Uuid, token: &str) -> Option<AuthenticatedRegistration> {
        let _lifecycle = self
            .node_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        self.purge_expired_locked(now);
        let candidate = BearerToken::new(token.to_owned()).ok()?;
        let candidate = token_digest(candidate.expose_for_authorization());
        for mut registration in self.registrations.iter_mut() {
            if registration.node_name != node_name {
                continue;
            }
            if let Some(bearer_generation) =
                registration
                    .token
                    .authenticate(&candidate, now, self.token_ttl)
            {
                return Some(AuthenticatedRegistration {
                    registration_id: *registration.key(),
                    bearer_generation,
                });
            }
        }
        None
    }

    pub fn stop_node(&self, registration_id: u64, bearer_generation: u64) -> Result<()> {
        let _lifecycle = self
            .node_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let registration = self
            .registrations
            .get(&registration_id)
            .ok_or_else(|| anyhow!("registration no longer exists"))?;
        anyhow::ensure!(
            registration.token.generation == bearer_generation,
            "bearer generation changed"
        );
        drop(registration);
        self.stop_nodes_for_registration(registration_id);
        self.registrations.remove(&registration_id);
        Ok(())
    }

    fn stop_nodes_for_registration(&self, registration_id: u64) {
        let node_ids = self
            .nodes
            .iter()
            .filter(|node| node.registration_id == registration_id)
            .map(|node| *node.key())
            .collect::<Vec<_>>();
        for node_id in node_ids {
            self.nodes.remove(&node_id);
        }
    }

    pub fn purge_expired(&self) -> usize {
        let _lifecycle = self
            .node_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.purge_expired_locked(Instant::now())
    }

    fn purge_expired_locked(&self, now: Instant) -> usize {
        let expired = self
            .registrations
            .iter()
            .filter(|registered| registered.token.is_expired(now))
            .map(|registered| *registered.key())
            .collect::<Vec<_>>();
        for registration_id in &expired {
            self.registrations.remove(registration_id);
            self.stop_nodes_for_registration(*registration_id);
        }
        expired.len()
    }

    pub fn add_module(
        &self,
        registration_id: u64,
        bearer_generation: u64,
        bytes: Vec<u8>,
    ) -> Result<u64> {
        // Authentication happens before Axum buffers the request body. Recheck
        // the lifecycle state at the mutation boundary so a slow upload cannot
        // outlive a rotation, stop, or lease expiry.
        let _lifecycle = self
            .node_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.purge_expired_locked(Instant::now());
        let registration = self
            .registrations
            .get(&registration_id)
            .ok_or_else(|| anyhow!("registration no longer exists"))?;
        anyhow::ensure!(
            registration.token.generation == bearer_generation,
            "bearer generation changed"
        );
        drop(registration);

        let id = self.next_module_id.fetch_add(1, atomic::Ordering::Relaxed);
        self.modules.insert(id, bytes);
        Ok(id)
    }
}

fn token_digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn issue_bearer() -> Result<(WireBearerToken, [u8; 32])> {
    let token = WireBearerToken::generate().map_err(|_| anyhow!("node bearer issuance failed"))?;
    let digest = token_digest(token.expose_for_wire());
    Ok((token, digest))
}

fn prepare_app(public_origin: String) -> Result<(Router, Arc<ControlServer>)> {
    // This server keeps registrations and numeric node IDs only in memory. Rotate the CA with
    // that state so a leaf issued before a restart can never regain a reused numeric identity.
    let ca_cert = CertificateAuthority::generate()?;
    let ca_cert_str = ca_cert.certificate_pem().to_owned();
    let (ctrl_cert, ctrl_pk) =
        lunatic_distributed::control::cert::default_server_certificates(&ca_cert)?;
    let quic_client =
        lunatic_distributed::quic::new_quic_client(&ca_cert_str, &ctrl_cert, &ctrl_pk)?;
    let control = Arc::new(ControlServer::new(ca_cert, quic_client, public_origin));
    let app = Router::new()
        .nest("/", routes::init_routes())
        .layer(Extension(control.clone()));
    Ok((app, control))
}

pub async fn control_server(http_socket: SocketAddr) -> Result<()> {
    anyhow::ensure!(
        http_socket.ip().is_loopback(),
        "plain HTTP node control is restricted to loopback"
    );
    control_server_from_tcp(TcpListener::bind(http_socket)?).await
}

pub async fn control_server_from_tcp(listener: TcpListener) -> Result<()> {
    let local_addr = listener.local_addr()?;
    anyhow::ensure!(
        local_addr.ip().is_loopback(),
        "plain HTTP node control is restricted to loopback"
    );
    let (app, control) = prepare_app(format!("http://{local_addr}/"))?;
    tokio::spawn(expired_registration_sweeper(Arc::downgrade(&control)));

    axum::Server::from_tcp(listener)?
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

async fn expired_registration_sweeper(control: Weak<ControlServer>) {
    loop {
        tokio::time::sleep(EXPIRED_REGISTRATION_SWEEP_INTERVAL).await;
        let Some(control) = control.upgrade() else {
            return;
        };
        control.purge_expired();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_server(token_ttl: Duration) -> Result<ControlServer> {
        let ca_cert_str = lunatic_distributed::distributed::server::test_root_cert();
        let ca_cert = lunatic_distributed::control::cert::test_root_cert()?;
        let (ctrl_cert, ctrl_pk) =
            lunatic_distributed::control::cert::default_server_certificates(&ca_cert)?;
        let quic_client =
            lunatic_distributed::quic::new_quic_client(&ca_cert_str, &ctrl_cert, &ctrl_pk)?;
        Ok(ControlServer::new_with_token_ttl(
            ca_cert,
            quic_client,
            "http://127.0.0.1:3030/".to_owned(),
            token_ttl,
        ))
    }

    fn register(server: &ControlServer, node_name: Uuid) -> Result<WireBearerToken> {
        let csr_pem = lunatic_distributed::distributed::server::gen_node_cert(
            &node_name.hyphenated().to_string(),
        )?
        .serialize_request_pem()?;
        server.register(&Register { node_name, csr_pem }, "provisional-certificate")
    }

    #[tokio::test]
    async fn verifier_accepts_only_the_issued_bearer_and_debug_is_redacted() -> Result<()> {
        let server = test_server(Duration::from_secs(60))?;
        let node_name = Uuid::from_u128(7);
        let bearer = register(&server, node_name)?;
        let raw = bearer.expose_for_wire();

        let authenticated = server
            .authenticate(node_name, raw)
            .expect("issued bearer authenticates");
        let mut beginning_mutation = raw.as_bytes().to_vec();
        beginning_mutation[0] ^= 1;
        let beginning_mutation = String::from_utf8(beginning_mutation)?;
        let mut ending_mutation = raw.as_bytes().to_vec();
        *ending_mutation.last_mut().expect("non-empty bearer") ^= 1;
        let ending_mutation = String::from_utf8(ending_mutation)?;

        assert!(server
            .authenticate(node_name, &beginning_mutation)
            .is_none());
        assert!(server.authenticate(node_name, &ending_mutation).is_none());
        let debug = format!(
            "{:?}",
            server
                .registrations
                .get(&authenticated.registration_id)
                .expect("registration")
                .value()
        );
        assert!(!debug.contains(raw));
        assert!(debug.contains("[REDACTED VERIFIER]"));
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_registration_is_rejected_and_stop_revokes_the_bearer() -> Result<()> {
        let server = test_server(Duration::from_secs(60))?;
        let node_name = Uuid::from_u128(8);
        let first = register(&server, node_name)?;
        assert!(server
            .authenticate(node_name, first.expose_for_wire())
            .is_some());

        assert!(register(&server, node_name).is_err());
        assert!(server
            .authenticate(node_name, first.expose_for_wire())
            .is_some());
        let authenticated = server
            .authenticate(node_name, first.expose_for_wire())
            .expect("original bearer remains authoritative");
        assert_eq!(server.registrations.len(), 1);

        server.stop_node(
            authenticated.registration_id,
            authenticated.bearer_generation,
        )?;
        assert!(server
            .authenticate(node_name, first.expose_for_wire())
            .is_none());
        assert!(server.registrations.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn expired_registration_retires_its_active_node() -> Result<()> {
        let server = test_server(Duration::from_millis(10))?;
        let node_name = Uuid::from_u128(9);
        let bearer = register(&server, node_name)?;
        let authenticated = server
            .authenticate(node_name, bearer.expose_for_wire())
            .expect("registration");
        let (node_id, _, _) = server.start_node(
            authenticated.registration_id,
            authenticated.bearer_generation,
            NodeStart {
                node_address: "127.0.0.1:4000".parse()?,
                attributes: HashMap::new(),
            },
        )?;

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(server.purge_expired(), 1);
        assert!(server.registrations.is_empty());
        assert!(!server.nodes.contains_key(&node_id));
        Ok(())
    }

    #[tokio::test]
    async fn delayed_start_and_rotation_recheck_lease_at_commit() -> Result<()> {
        let server = test_server(Duration::from_millis(10))?;
        let start_name = Uuid::from_u128(12);
        let start_bearer = register(&server, start_name)?;
        let stale_start = server
            .authenticate(start_name, start_bearer.expose_for_wire())
            .expect("start registration");
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(server
            .start_node(
                stale_start.registration_id,
                stale_start.bearer_generation,
                NodeStart {
                    node_address: "127.0.0.1:4001".parse()?,
                    attributes: HashMap::new(),
                },
            )
            .is_err());
        assert!(server.registrations.is_empty());
        assert!(server.nodes.is_empty());

        let rotate_name = Uuid::from_u128(13);
        let rotate_bearer = register(&server, rotate_name)?;
        let stale_rotation = server
            .authenticate(rotate_name, rotate_bearer.expose_for_wire())
            .expect("rotation registration");
        let next = WireBearerToken::generate().map_err(|error| anyhow!(error))?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(server
            .stage_bearer_rotation(
                stale_rotation.registration_id,
                stale_rotation.bearer_generation,
                &next,
            )
            .is_err());
        assert!(server.registrations.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn staged_rotation_is_idempotent_and_promotes_only_on_next_token_use() -> Result<()> {
        let server = test_server(Duration::from_secs(60))?;
        let node_name = Uuid::from_u128(10);
        let current = register(&server, node_name)?;
        let authenticated = server
            .authenticate(node_name, current.expose_for_wire())
            .expect("current bearer");
        let next = WireBearerToken::generate().map_err(|error| anyhow!(error))?;

        assert_eq!(
            server.stage_bearer_rotation(
                authenticated.registration_id,
                authenticated.bearer_generation,
                &next,
            )?,
            1
        );
        assert_eq!(
            server.stage_bearer_rotation(
                authenticated.registration_id,
                authenticated.bearer_generation,
                &next,
            )?,
            1
        );
        let conflicting = WireBearerToken::generate().map_err(|error| anyhow!(error))?;
        assert!(server
            .stage_bearer_rotation(
                authenticated.registration_id,
                authenticated.bearer_generation,
                &conflicting,
            )
            .is_err());
        assert_eq!(
            server
                .authenticate(node_name, current.expose_for_wire())
                .expect("current remains valid until acknowledgement")
                .bearer_generation,
            0
        );
        assert_eq!(
            server
                .authenticate(node_name, next.expose_for_wire())
                .expect("next bearer promotes pending rotation")
                .bearer_generation,
            1
        );
        assert!(server
            .authenticate(node_name, current.expose_for_wire())
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn module_insert_rechecks_generation_and_registration_at_commit() -> Result<()> {
        let server = test_server(Duration::from_secs(60))?;
        let node_name = Uuid::from_u128(11);
        let current = register(&server, node_name)?;
        let stale = server
            .authenticate(node_name, current.expose_for_wire())
            .expect("current bearer");
        let next = WireBearerToken::generate().map_err(|error| anyhow!(error))?;
        server.stage_bearer_rotation(stale.registration_id, stale.bearer_generation, &next)?;
        let current = server
            .authenticate(node_name, next.expose_for_wire())
            .expect("next bearer promotes pending rotation");

        assert!(server
            .add_module(stale.registration_id, stale.bearer_generation, vec![1])
            .is_err());
        assert!(server.modules.is_empty());
        assert_eq!(
            server.add_module(current.registration_id, current.bearer_generation, vec![2])?,
            1
        );

        server.stop_node(current.registration_id, current.bearer_generation)?;
        assert!(server
            .add_module(current.registration_id, current.bearer_generation, vec![3])
            .is_err());
        assert_eq!(server.modules.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn non_loopback_plain_http_listener_is_rejected() {
        let error = control_server("0.0.0.0:0".parse().unwrap())
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "plain HTTP node control is restricted to loopback"
        );
    }
}
