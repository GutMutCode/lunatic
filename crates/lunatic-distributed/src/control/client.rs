use anyhow::{anyhow, Result};
use dashmap::DashMap;
use lunatic_control::api::{
    BearerToken, ControlUrls as WireControlUrls, ModuleBytes, ModuleId, NodeBearerRefresh,
    NodeRefreshed, NodeStart, NodeStarted, NodesList, Register, RegistrationResponse,
    WireBearerToken,
};
use lunatic_control::NodeInfo;
use lunatic_process::runtimes::RawWasm;
use reqwest::{
    header::{self, HeaderValue},
    redirect, Client as HttpClient, Response, Url,
};
use serde::de::DeserializeOwned;
use std::{
    collections::HashMap,
    fmt,
    net::SocketAddr,
    sync::{atomic, atomic::AtomicBool, atomic::AtomicU64, atomic::AtomicUsize, Arc, RwLock, Weak},
    time::Duration,
};
use zeroize::Zeroizing;

pub const DEFAULT_MAX_TOPOLOGY_NODES: usize = 1_024;
const CONTROL_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_TOPOLOGY_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const CONTROL_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_CONTROL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_MODULE_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// Validated runtime registration.
///
/// Current and at most one pending bearer are held in zeroizing storage and
/// deliberately cannot be cloned or serialized into general configuration.
///
/// ```compile_fail
/// fn assert_clone<T: Clone>() {}
/// assert_clone::<lunatic_distributed::control::Registration>();
/// ```
///
/// ```compile_fail
/// fn assert_serialize<T: serde::Serialize>() {}
/// assert_serialize::<lunatic_distributed::control::Registration>();
/// ```
pub struct Registration {
    metadata: RegistrationMetadata,
    bearer: BearerToken,
    pending_bearer: Option<BearerToken>,
    bearer_generation: u64,
    bearer_expires_in_seconds: u64,
    urls: ValidatedControlUrls,
}

impl fmt::Debug for Registration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registration")
            .field("metadata", &self.metadata)
            .field("bearer", &"[REDACTED]")
            .field(
                "pending_bearer",
                &self.pending_bearer.as_ref().map(|_| "[REDACTED]"),
            )
            .field("bearer_generation", &self.bearer_generation)
            .field("bearer_expires_in_seconds", &self.bearer_expires_in_seconds)
            .field("origin", &self.urls.origin())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct RegistrationMetadata {
    pub node_name: uuid::Uuid,
    pub cert_pem_chain: Vec<String>,
    pub root_cert: String,
    pub envs: Vec<i64>,
    pub is_privileged: bool,
}

#[derive(Clone)]
struct ValidatedControlUrls {
    origin: Url,
    nodes: Url,
    node_started: Url,
    node_refreshed: Url,
    node_stopped: Url,
    add_module: Url,
    get_nodes: Url,
}

impl ValidatedControlUrls {
    fn origin(&self) -> &str {
        self.origin.as_str()
    }

    fn module(&self, module_id: u64) -> Result<Url> {
        self.origin
            .join(&format!("module/{module_id}"))
            .map_err(|_| anyhow!("control_endpoint_invalid"))
    }

    fn ensure_target(&self, target: &Url) -> Result<()> {
        ensure_same_origin(&self.origin, target)
    }
}

#[derive(Clone)]
pub struct Client {
    // Worker-only clients deliberately carry no public lifetime guard. All
    // public clones share one guard, whose final drop revokes local authority
    // even if a worker temporarily holds `InnerClient` strongly.
    _public_lifetime: Option<Arc<PublicClientLifetime>>,
    inner: Arc<InnerClient>,
}

struct PublicClientLifetime {
    inner: Arc<InnerClient>,
}

impl Drop for PublicClientLifetime {
    fn drop(&mut self) {
        self.inner.stop_locally();
    }
}

pub struct InnerClient {
    reg: RwLock<Registration>,
    node_id: u64,
    http_client: HttpClient,
    stopped: AtomicBool,
    bearer_rotation: tokio::sync::Mutex<()>,
    worker_stop: tokio::sync::watch::Sender<bool>,
    bearer_refresh_after_seconds: AtomicU64,
    next_message_id: AtomicU64,
    next_query_id: AtomicU64,
    node_queries: DashMap<u64, Vec<u64>>,
    pending_node_queries: AtomicUsize,
    nodes: DashMap<u64, NodeInfo>,
    node_ids: RwLock<Vec<u64>>,
    max_topology_nodes: usize,
}

impl InnerClient {
    fn stop_locally(&self) {
        self.stopped.store(true, atomic::Ordering::Release);
        let _ = self.worker_stop.send(true);
        self.reg
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .revoke();
    }
}

struct PendingNodeQueryReservation {
    inner: Arc<InnerClient>,
    active: bool,
}

impl PendingNodeQueryReservation {
    fn commit(mut self) {
        self.active = false;
    }
}

impl Drop for PendingNodeQueryReservation {
    fn drop(&mut self) {
        if self.active {
            self.inner
                .pending_node_queries
                .fetch_sub(1, atomic::Ordering::AcqRel);
        }
    }
}

impl Registration {
    #[doc(hidden)]
    pub fn from_response(control_url: Url, response: RegistrationResponse) -> Result<Self> {
        let origin = validate_registration_origin(&control_url)?;
        let urls = validate_control_urls(origin, &response.urls)?;
        Ok(Self {
            metadata: RegistrationMetadata {
                node_name: response.node_name,
                cert_pem_chain: response.cert_pem_chain,
                root_cert: response.root_cert,
                envs: response.envs,
                is_privileged: response.is_privileged,
            },
            bearer: response.authentication_token.into_runtime(),
            pending_bearer: None,
            bearer_generation: response.bearer_generation,
            bearer_expires_in_seconds: validate_bearer_ttl(response.bearer_expires_in_seconds)?,
            urls,
        })
    }

    pub fn metadata(&self) -> RegistrationMetadata {
        self.metadata.clone()
    }

    pub fn is_privileged(&self) -> bool {
        self.metadata.is_privileged
    }

    pub fn envs(&self) -> &[i64] {
        &self.metadata.envs
    }

    fn authorization_header(&self) -> Result<HeaderValue> {
        if self.bearer.is_revoked() {
            return Err(anyhow!("control_registration_revoked"));
        }
        authorization_header(&self.bearer)
    }

    fn install_started(&mut self, started: NodeStarted) -> Result<u64> {
        if started.cert_pem_chain.is_empty() {
            return Err(anyhow!("control_started_certificate_missing"));
        }
        let node_id = u64::try_from(started.node_id)
            .map_err(|_| anyhow!("control_started_identity_invalid"))?;
        if started.bearer_generation != self.bearer_generation {
            return Err(anyhow!("control_started_bearer_generation_mismatch"));
        }
        let bearer_expires_in_seconds = validate_bearer_ttl(started.bearer_expires_in_seconds)?;
        self.metadata.cert_pem_chain = started.cert_pem_chain;
        self.bearer_expires_in_seconds = bearer_expires_in_seconds;
        Ok(node_id)
    }

    fn rotation_request(&mut self) -> Result<NodeBearerRefresh> {
        let next_authentication_token = if let Some(pending) = &self.pending_bearer {
            WireBearerToken::new(pending.expose_for_authorization().to_owned())
                .map_err(|_| anyhow!("control_bearer_rotation_invalid"))?
        } else {
            let wire = WireBearerToken::generate()
                .map_err(|_| anyhow!("control_bearer_rotation_generation_failed"))?;
            self.pending_bearer = Some(
                wire.runtime_copy()
                    .map_err(|_| anyhow!("control_bearer_rotation_invalid"))?,
            );
            wire
        };
        Ok(NodeBearerRefresh {
            current_bearer_generation: self.bearer_generation,
            next_authentication_token,
        })
    }

    fn install_refreshed(&mut self, refreshed: NodeRefreshed) -> Result<()> {
        let expected_generation = self
            .bearer_generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("control_bearer_generation_exhausted"))?;
        if refreshed.next_bearer_generation != expected_generation {
            return Err(anyhow!("control_bearer_generation_mismatch"));
        }
        let bearer_expires_in_seconds = validate_bearer_ttl(refreshed.bearer_expires_in_seconds)?;
        let pending = self
            .pending_bearer
            .take()
            .ok_or_else(|| anyhow!("control_bearer_rotation_missing"))?;
        self.bearer.revoke();
        self.bearer = pending;
        self.bearer_generation = refreshed.next_bearer_generation;
        self.bearer_expires_in_seconds = bearer_expires_in_seconds;
        Ok(())
    }

    fn refresh_after_seconds(&self) -> u64 {
        (self.bearer_expires_in_seconds / 2).max(1)
    }

    fn revoke(&mut self) {
        self.bearer.revoke();
        if let Some(pending) = &mut self.pending_bearer {
            pending.revoke();
        }
        self.pending_bearer = None;
    }
}

fn validate_bearer_ttl(ttl: u64) -> Result<u64> {
    if ttl == 0 || ttl > 24 * 60 * 60 {
        return Err(anyhow!("control_bearer_ttl_invalid"));
    }
    Ok(ttl)
}

fn validate_registration_origin(control_url: &Url) -> Result<Url> {
    if control_url.username() != ""
        || control_url.password().is_some()
        || control_url.query().is_some()
        || control_url.fragment().is_some()
        || !matches!(control_url.path(), "" | "/")
    {
        return Err(anyhow!("control_origin_invalid"));
    }

    match control_url.scheme() {
        "https" => {}
        "http" => {
            let is_loopback = control_url
                .host_str()
                .map(|host| host.trim_start_matches('[').trim_end_matches(']'))
                .and_then(|host| host.parse::<std::net::IpAddr>().ok())
                .is_some_and(|host| host.is_loopback());
            if !is_loopback {
                return Err(anyhow!("control_transport_insecure"));
            }
        }
        _ => return Err(anyhow!("control_transport_insecure")),
    }

    let mut origin = control_url.clone();
    origin.set_path("/");
    origin.set_query(None);
    origin.set_fragment(None);
    Ok(origin)
}

fn validate_control_urls(origin: Url, supplied: &WireControlUrls) -> Result<ValidatedControlUrls> {
    validate_endpoint(&origin, &supplied.api_base, "/")?;
    validate_endpoint(&origin, &supplied.nodes, "/nodes")?;
    validate_endpoint(&origin, &supplied.node_started, "/started")?;
    validate_endpoint(&origin, &supplied.node_refreshed, "/refreshed")?;
    validate_endpoint(&origin, &supplied.node_stopped, "/stopped")?;
    validate_endpoint(&origin, &supplied.add_module, "/module")?;
    validate_endpoint(&origin, &supplied.get_nodes, "/nodes")?;

    if supplied.get_module.matches("{id}").count() != 1 {
        return Err(anyhow!("control_endpoint_invalid"));
    }
    validate_endpoint(
        &origin,
        &supplied.get_module.replace("{id}", "0"),
        "/module/0",
    )?;

    Ok(ValidatedControlUrls {
        nodes: origin
            .join("nodes")
            .map_err(|_| anyhow!("control_endpoint_invalid"))?,
        node_started: origin
            .join("started")
            .map_err(|_| anyhow!("control_endpoint_invalid"))?,
        node_refreshed: origin
            .join("refreshed")
            .map_err(|_| anyhow!("control_endpoint_invalid"))?,
        node_stopped: origin
            .join("stopped")
            .map_err(|_| anyhow!("control_endpoint_invalid"))?,
        add_module: origin
            .join("module")
            .map_err(|_| anyhow!("control_endpoint_invalid"))?,
        get_nodes: origin
            .join("nodes")
            .map_err(|_| anyhow!("control_endpoint_invalid"))?,
        origin,
    })
}

fn validate_endpoint(origin: &Url, raw: &str, expected_path: &str) -> Result<()> {
    let target = Url::parse(raw).map_err(|_| anyhow!("control_endpoint_invalid"))?;
    if target.username() != ""
        || target.password().is_some()
        || target.query().is_some()
        || target.fragment().is_some()
        || target.path() != expected_path
    {
        return Err(anyhow!("control_endpoint_invalid"));
    }
    ensure_same_origin(origin, &target)
}

fn ensure_same_origin(origin: &Url, target: &Url) -> Result<()> {
    let same = origin.scheme() == target.scheme()
        && origin
            .host_str()
            .zip(target.host_str())
            .is_some_and(|(expected, actual)| expected.eq_ignore_ascii_case(actual))
        && origin.port_or_known_default() == target.port_or_known_default();
    if !same {
        return Err(anyhow!("control_endpoint_origin_mismatch"));
    }
    Ok(())
}

fn control_http_client() -> Result<HttpClient> {
    HttpClient::builder()
        .redirect(redirect::Policy::none())
        .no_proxy()
        .connect_timeout(CONTROL_CONNECT_TIMEOUT)
        .timeout(CONTROL_REQUEST_TIMEOUT)
        .build()
        .map_err(|_| anyhow!("control_http_client_failed"))
}

fn authorization_header(token: &BearerToken) -> Result<HeaderValue> {
    let encoded = Zeroizing::new(format!("Bearer {}", token.expose_for_authorization()));
    let mut value = HeaderValue::from_str(encoded.as_str())
        .map_err(|_| anyhow!("control_bearer_header_invalid"))?;
    value.set_sensitive(true);
    Ok(value)
}

async fn decode_response<T: DeserializeOwned>(
    mut response: Response,
    operation: &str,
    max_bytes: usize,
) -> Result<T> {
    let status = response.status();
    if status.is_redirection() {
        return Err(anyhow!("control_{operation}_redirect_forbidden"));
    }
    if !status.is_success() {
        return Err(anyhow!(
            "control_{operation}_http_status_{}",
            status.as_u16()
        ));
    }
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(anyhow!("control_{operation}_response_too_large"));
    }
    let initial_capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or(0)
        .min(max_bytes);
    let mut body = Zeroizing::new(Vec::with_capacity(initial_capacity));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| anyhow!("control_{operation}_response_invalid"))?
    {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(anyhow!("control_{operation}_response_too_large"));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| anyhow!("control_{operation}_response_invalid"))
}

impl Client {
    fn public(inner: Arc<InnerClient>) -> Self {
        Self {
            _public_lifetime: Some(Arc::new(PublicClientLifetime {
                inner: Arc::clone(&inner),
            })),
            inner,
        }
    }

    fn worker(inner: Arc<InnerClient>) -> Self {
        Self {
            _public_lifetime: None,
            inner,
        }
    }

    /// Construct a client from an already-known static node topology.
    ///
    /// This avoids an HTTP control-plane dependency for embedded deployments and
    /// deterministic node-to-node protocol tests.
    #[doc(hidden)]
    pub fn from_static_nodes(
        metadata: RegistrationMetadata,
        node_id: u64,
        nodes: Vec<NodeInfo>,
    ) -> Self {
        let origin = Url::parse("http://127.0.0.1:1/").expect("static loopback URL");
        let urls = ValidatedControlUrls {
            nodes: origin.join("nodes").expect("static nodes URL"),
            node_started: origin.join("started").expect("static start URL"),
            node_refreshed: origin.join("refreshed").expect("static refresh URL"),
            node_stopped: origin.join("stopped").expect("static stop URL"),
            add_module: origin.join("module").expect("static module URL"),
            get_nodes: origin.join("nodes").expect("static query URL"),
            origin,
        };
        let reg = Registration {
            metadata,
            bearer: WireBearerToken::generate()
                .expect("static topology bearer generation")
                .into_runtime(),
            pending_bearer: None,
            bearer_generation: 0,
            bearer_expires_in_seconds: 300,
            urls,
        };
        let mut node_ids = nodes.iter().map(|node| node.id).collect::<Vec<_>>();
        node_ids.sort_unstable();
        node_ids.dedup();
        let node_map = DashMap::new();
        for node in nodes {
            node_map.insert(node.id, node);
        }
        let refresh_after = reg.refresh_after_seconds();
        let (worker_stop, _) = tokio::sync::watch::channel(false);
        Self::public(Arc::new(InnerClient {
            reg: RwLock::new(reg),
            node_id,
            http_client: control_http_client().expect("hardened control HTTP client"),
            stopped: AtomicBool::new(false),
            bearer_rotation: tokio::sync::Mutex::new(()),
            worker_stop,
            bearer_refresh_after_seconds: AtomicU64::new(refresh_after),
            next_message_id: AtomicU64::new(1),
            next_query_id: AtomicU64::new(1),
            node_queries: DashMap::new(),
            pending_node_queries: AtomicUsize::new(0),
            nodes: node_map,
            node_ids: RwLock::new(node_ids),
            max_topology_nodes: DEFAULT_MAX_TOPOLOGY_NODES,
        }))
    }

    pub async fn new(
        reg: Registration,
        node_address: SocketAddr,
        attributes: HashMap<String, String>,
    ) -> Result<Self> {
        Self::new_with_topology_limit(reg, node_address, attributes, DEFAULT_MAX_TOPOLOGY_NODES)
            .await
    }

    pub async fn new_with_topology_limit(
        mut reg: Registration,
        node_address: SocketAddr,
        attributes: HashMap<String, String>,
        max_topology_nodes: usize,
    ) -> Result<Self> {
        let http_client = control_http_client()?;
        let started = Self::start(
            &http_client,
            &reg,
            NodeStart {
                node_address,
                attributes,
            },
        )
        .await;
        let started = match started {
            Ok(started) => started,
            Err(error) => {
                let _ = Self::stop_registration(&http_client, &reg).await;
                reg.revoke();
                return Err(error);
            }
        };
        let node_id = match reg.install_started(started) {
            Ok(node_id) => node_id,
            Err(error) => {
                let _ = Self::stop_registration(&http_client, &reg).await;
                reg.revoke();
                return Err(error);
            }
        };
        let refresh_after = reg.refresh_after_seconds();
        let (worker_stop, _) = tokio::sync::watch::channel(false);

        let client = Client::public(Arc::new(InnerClient {
            reg: RwLock::new(reg),
            node_id,
            http_client,
            stopped: AtomicBool::new(false),
            bearer_rotation: tokio::sync::Mutex::new(()),
            worker_stop,
            bearer_refresh_after_seconds: AtomicU64::new(refresh_after),
            next_message_id: AtomicU64::new(1),
            node_queries: DashMap::new(),
            pending_node_queries: AtomicUsize::new(0),
            next_query_id: AtomicU64::new(1),
            nodes: Default::default(),
            node_ids: Default::default(),
            max_topology_nodes,
        }));

        if let Err(error) = client.refresh_nodes().await {
            let _ = client.shutdown().await;
            return Err(error);
        }
        tokio::task::spawn(refresh_nodes_task(
            Arc::downgrade(&client.inner),
            client.inner.worker_stop.subscribe(),
        ));
        tokio::task::spawn(refresh_bearer_task(
            Arc::downgrade(&client.inner),
            client.inner.worker_stop.subscribe(),
        ));

        Ok(client)
    }

    pub async fn register(
        control_url: Url,
        node_name: uuid::Uuid,
        csr_pem: String,
    ) -> Result<Registration> {
        let http_client = control_http_client()?;
        let validated_url = validate_registration_origin(&control_url)?;
        let reg = Register { node_name, csr_pem };
        Self::send_registration(&http_client, validated_url, reg).await
    }

    pub fn reg(&self) -> RegistrationMetadata {
        self.inner
            .reg
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .metadata()
    }

    pub fn node_id(&self) -> u64 {
        self.inner.node_id
    }

    pub fn next_message_id(&self) -> u64 {
        self.inner
            .next_message_id
            .fetch_add(1, atomic::Ordering::Relaxed)
    }

    pub fn next_query_id(&self) -> u64 {
        self.inner
            .next_query_id
            .fetch_add(1, atomic::Ordering::Relaxed)
    }

    async fn send_registration(
        client: &HttpClient,
        url: Url,
        reg: Register,
    ) -> Result<Registration> {
        let response = client
            .post(url.clone())
            .json(&reg)
            .send()
            .await
            .map_err(|_| anyhow!("control_registration_transport_failed"))?;
        let response: RegistrationResponse =
            decode_response(response, "registration", MAX_CONTROL_RESPONSE_BYTES).await?;
        Registration::from_response(url, response)
    }

    async fn start(
        client: &HttpClient,
        reg: &Registration,
        start: NodeStart,
    ) -> Result<NodeStarted> {
        let response = client
            .post(reg.urls.node_started.clone())
            .json(&start)
            .header(header::AUTHORIZATION, reg.authorization_header()?)
            .header(
                "x-lunatic-node-name",
                reg.metadata.node_name.hyphenated().to_string(),
            )
            .send()
            .await
            .map_err(|_| anyhow!("control_start_transport_failed"))?;
        decode_response(response, "start", MAX_CONTROL_RESPONSE_BYTES).await
    }

    async fn stop_registration(client: &HttpClient, reg: &Registration) -> Result<()> {
        let response = client
            .post(reg.urls.node_stopped.clone())
            .json(&())
            .header(header::AUTHORIZATION, reg.authorization_header()?)
            .header(
                "x-lunatic-node-name",
                reg.metadata.node_name.hyphenated().to_string(),
            )
            .send()
            .await
            .map_err(|_| anyhow!("control_stop_transport_failed"))?;
        decode_response::<()>(response, "stop", MAX_CONTROL_RESPONSE_BYTES).await
    }

    fn request_context(&self, url: &Url) -> Result<(HeaderValue, uuid::Uuid)> {
        if self.inner.stopped.load(atomic::Ordering::Acquire) {
            return Err(anyhow!("control_registration_revoked"));
        }
        let reg = self
            .inner
            .reg
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reg.urls.ensure_target(url)?;
        Ok((reg.authorization_header()?, reg.metadata.node_name))
    }

    async fn get<T: DeserializeOwned>(
        &self,
        mut url: Url,
        query: Option<&str>,
        max_bytes: usize,
    ) -> Result<T> {
        url.set_query(query);
        let (authorization, node_name) = self.request_context(&url)?;
        let response = self
            .inner
            .http_client
            .get(url)
            .header(header::AUTHORIZATION, authorization)
            .header("x-lunatic-node-name", node_name.hyphenated().to_string())
            .send()
            .await
            .map_err(|_| anyhow!("control_get_transport_failed"))?;
        decode_response(response, "get", max_bytes).await
    }

    async fn upload<R: DeserializeOwned>(&self, url: Url, body: Vec<u8>) -> Result<R> {
        let (authorization, node_name) = self.request_context(&url)?;
        let response = self
            .inner
            .http_client
            .post(url)
            .body(body)
            .header(header::AUTHORIZATION, authorization)
            .header("x-lunatic-node-name", node_name.hyphenated().to_string())
            .send()
            .await
            .map_err(|_| anyhow!("control_upload_transport_failed"))?;
        decode_response(response, "upload", MAX_CONTROL_RESPONSE_BYTES).await
    }

    pub async fn refresh_nodes(&self) -> Result<()> {
        let url = self
            .inner
            .reg
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .urls
            .nodes
            .clone();
        let resp: NodesList = self.get(url, None, MAX_CONTROL_RESPONSE_BYTES).await?;
        anyhow::ensure!(
            resp.nodes.len() <= self.inner.max_topology_nodes,
            "Control topology contains {} nodes, exceeding configured limit {}",
            resp.nodes.len(),
            self.inner.max_topology_nodes
        );
        self.replace_nodes(resp.nodes);
        Ok(())
    }

    fn replace_nodes(&self, nodes: Vec<NodeInfo>) {
        let mut node_ids = Vec::with_capacity(nodes.len());
        for node in nodes {
            let id = node.id;
            node_ids.push(id);
            self.inner.nodes.insert(id, node);
        }
        node_ids.sort_unstable();
        node_ids.dedup();
        let active = node_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        self.inner
            .nodes
            .retain(|node_id, _| active.contains(node_id));
        if let Ok(mut self_node_ids) = self.inner.node_ids.write() {
            *self_node_ids = node_ids;
        }
    }

    pub async fn refresh_bearer(&self) -> Result<()> {
        let _rotation = self.inner.bearer_rotation.lock().await;
        if self.inner.stopped.load(atomic::Ordering::Acquire) {
            return Err(anyhow!("control_registration_revoked"));
        }
        let (url, authorization, node_name, request) = {
            let mut reg = self
                .inner
                .reg
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let request = reg.rotation_request()?;
            (
                reg.urls.node_refreshed.clone(),
                reg.authorization_header()?,
                reg.metadata.node_name,
                request,
            )
        };
        let response = self
            .inner
            .http_client
            .post(url)
            .json(&request)
            .header(header::AUTHORIZATION, authorization)
            .header("x-lunatic-node-name", node_name.hyphenated().to_string())
            .send()
            .await
            .map_err(|_| anyhow!("control_refresh_transport_failed"))?;
        let refreshed: NodeRefreshed =
            decode_response(response, "refresh", MAX_CONTROL_RESPONSE_BYTES).await?;
        let refresh_after = {
            let mut reg = self
                .inner
                .reg
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            reg.install_refreshed(refreshed)?;
            reg.refresh_after_seconds()
        };
        self.inner
            .bearer_refresh_after_seconds
            .store(refresh_after, atomic::Ordering::Release);
        // Present the acknowledged bearer immediately. The server promotes
        // its pending verifier during authentication, minimizing the interval
        // in which the replaced current bearer remains authoritative.
        if self.refresh_nodes().await.is_err() {
            // The server may have promoted the pending digest even when this
            // confirmation response is lost. The installed bearer remains
            // authoritative and the ordinary topology worker will present it
            // again; do not start another rotation at retry frequency.
            log::warn!("Node-control bearer promotion confirmation failed");
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        if self.inner.stopped.swap(true, atomic::Ordering::AcqRel) {
            return Ok(());
        }
        let _ = self.inner.worker_stop.send(true);
        let (url, authorization, node_name) = {
            let mut reg = self
                .inner
                .reg
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let context = (
                reg.urls.node_stopped.clone(),
                reg.authorization_header(),
                reg.metadata.node_name,
            );
            reg.revoke();
            context
        };
        let authorization = authorization?;
        // Local authority and workers are already revoked, so cancellation
        // while waiting for an in-flight rotation is safe. The lock only
        // orders the best-effort remote stop after that rotation settles.
        let _rotation = self.inner.bearer_rotation.lock().await;

        let response = self
            .inner
            .http_client
            .post(url)
            .json(&())
            .header(header::AUTHORIZATION, authorization)
            .header("x-lunatic-node-name", node_name.hyphenated().to_string())
            .send()
            .await
            .map_err(|_| anyhow!("control_stop_transport_failed"))?;
        decode_response::<()>(response, "stop", MAX_CONTROL_RESPONSE_BYTES).await
    }

    pub async fn notify_node_stopped(&self) -> Result<()> {
        self.shutdown().await
    }

    pub fn node_info(&self, node_id: u64) -> Option<NodeInfo> {
        self.inner.nodes.get(&node_id).map(|e| e.clone())
    }

    pub fn node_ids(&self) -> Vec<u64> {
        self.inner.node_ids.read().unwrap().clone()
    }

    fn reserve_node_query(&self) -> Result<PendingNodeQueryReservation> {
        self.inner
            .pending_node_queries
            .fetch_update(
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
                |current| (current < self.inner.max_topology_nodes).then_some(current + 1),
            )
            .map_err(|_| {
                anyhow!(
                    "Pending node query limit {} reached",
                    self.inner.max_topology_nodes
                )
            })?;
        Ok(PendingNodeQueryReservation {
            inner: self.inner.clone(),
            active: true,
        })
    }

    pub async fn lookup_nodes(&self, query: &str) -> Result<(u64, usize)> {
        let reservation = self.reserve_node_query()?;
        let url = self
            .inner
            .reg
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .urls
            .get_nodes
            .clone();
        let resp: NodesList = self
            .get(url, Some(query), MAX_CONTROL_RESPONSE_BYTES)
            .await?;
        anyhow::ensure!(
            resp.nodes.len() <= self.inner.max_topology_nodes,
            "Node query returned {} results, exceeding configured limit {}",
            resp.nodes.len(),
            self.inner.max_topology_nodes
        );
        let nodes: Vec<u64> = resp.nodes.into_iter().map(move |v| v.id).collect();
        let nodes_count = nodes.len();
        let query_id = self.next_query_id();
        self.inner.node_queries.insert(query_id, nodes);
        reservation.commit();
        Ok((query_id, nodes_count))
    }

    pub fn query_result(&self, query_id: &u64) -> Option<(u64, Vec<u64>)> {
        let result = self.inner.node_queries.remove(query_id);
        if result.is_some() {
            self.inner
                .pending_node_queries
                .fetch_sub(1, atomic::Ordering::AcqRel);
        }
        result
    }

    pub fn node_count(&self) -> usize {
        self.inner.node_ids.read().unwrap().len()
    }

    pub async fn get_module(&self, module_id: u64, environment_id: u64) -> Result<Vec<u8>> {
        log::info!("Get module {module_id}");
        let url = self
            .inner
            .reg
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .urls
            .module(module_id)?;
        let query = format!("env_id={environment_id}");
        let resp: ModuleBytes = self
            .get(url, Some(&query), MAX_MODULE_RESPONSE_BYTES)
            .await?;
        Ok(resp.bytes)
    }

    pub async fn add_module(&self, module: Vec<u8>) -> Result<RawWasm> {
        let url = self
            .inner
            .reg
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .urls
            .add_module
            .clone();
        let resp: ModuleId = self.upload(url, module.clone()).await?;
        Ok(RawWasm::new(Some(resp.module_id), module))
    }
}

async fn worker_wait(delay: Duration, stop: &mut tokio::sync::watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = stop.changed() => changed.is_err() || *stop.borrow(),
    }
}

async fn refresh_nodes_task(
    inner: Weak<InnerClient>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if worker_wait(CONTROL_TOPOLOGY_REFRESH_INTERVAL, &mut stop).await {
            return;
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if inner.stopped.load(atomic::Ordering::Acquire) {
            return;
        }
        let client = Client::worker(inner);
        let refreshed = tokio::select! {
            biased;
            _ = stop.changed() => return,
            refreshed = client.refresh_nodes() => refreshed,
        };
        if refreshed.is_err() {
            log::warn!("Node-control topology refresh failed");
        }
    }
}

async fn refresh_bearer_task(
    inner: Weak<InnerClient>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut retry = false;
    loop {
        let delay = if retry {
            CONTROL_RETRY_INTERVAL
        } else {
            let refresh_after = match inner.upgrade() {
                Some(inner) => inner
                    .bearer_refresh_after_seconds
                    .load(atomic::Ordering::Acquire),
                None => return,
            };
            Duration::from_secs(refresh_after.max(1))
        };
        if worker_wait(delay, &mut stop).await {
            return;
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if inner.stopped.load(atomic::Ordering::Acquire) {
            return;
        }
        let client = Client::worker(inner);
        let refreshed = tokio::select! {
            biased;
            _ = stop.changed() => return,
            refreshed = client.refresh_bearer() => refreshed,
        };
        retry = refreshed.is_err();
        if retry {
            log::warn!("Node-control bearer refresh failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lunatic_control::api::WireBearerToken;

    const TEST_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    fn wire_urls(base: &str) -> WireControlUrls {
        let base = base.trim_end_matches('/');
        WireControlUrls {
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

    fn registration_response(base: &str, token: &str) -> RegistrationResponse {
        RegistrationResponse {
            node_name: uuid::Uuid::from_u128(1),
            cert_pem_chain: Vec::new(),
            authentication_token: WireBearerToken::new(token).unwrap(),
            bearer_generation: 0,
            bearer_expires_in_seconds: 300,
            root_cert: String::new(),
            urls: wire_urls(base),
            envs: Vec::new(),
            is_privileged: true,
        }
    }

    fn registration() -> Registration {
        let base = "http://127.0.0.1:1/";
        Registration::from_response(
            Url::parse(base).unwrap(),
            registration_response(base, TEST_TOKEN),
        )
        .unwrap()
    }

    fn registration_metadata() -> RegistrationMetadata {
        registration().metadata()
    }

    fn node(id: u64, port: u16, name: &str) -> NodeInfo {
        NodeInfo {
            id,
            name: name.into(),
            address: ([127, 0, 0, 1], port).into(),
        }
    }

    #[test]
    fn topology_replacement_updates_metadata_and_removes_departed_nodes() {
        let client = Client::from_static_nodes(
            registration_metadata(),
            1,
            vec![node(1, 1001, "old-one"), node(2, 1002, "two")],
        );

        client.replace_nodes(vec![node(1, 2001, "new-one")]);

        assert_eq!(client.node_ids(), vec![1]);
        let current = client.node_info(1).expect("remaining node");
        assert_eq!(current.name, "new-one");
        assert_eq!(current.address.port(), 2001);
        assert!(client.node_info(2).is_none());
    }

    #[test]
    fn pending_query_capacity_is_released_on_cancellation_and_result_consumption() {
        let client =
            Client::from_static_nodes(registration_metadata(), 1, vec![node(1, 1001, "one")]);

        let cancelled = client.reserve_node_query().expect("query reservation");
        assert_eq!(
            client
                .inner
                .pending_node_queries
                .load(atomic::Ordering::Acquire),
            1
        );
        drop(cancelled);
        assert_eq!(
            client
                .inner
                .pending_node_queries
                .load(atomic::Ordering::Acquire),
            0
        );

        let completed = client.reserve_node_query().expect("query reservation");
        client.inner.node_queries.insert(7, vec![1]);
        completed.commit();
        assert_eq!(client.query_result(&7), Some((7, vec![1])));
        assert_eq!(
            client
                .inner
                .pending_node_queries
                .load(atomic::Ordering::Acquire),
            0
        );
    }

    #[test]
    fn started_response_installs_identity_bound_certificate() {
        let mut reg = registration();
        let node_id = reg
            .install_started(NodeStarted {
                node_id: 42,
                cert_pem_chain: vec!["node-42-certificate".into()],
                bearer_generation: 0,
                bearer_expires_in_seconds: 300,
            })
            .unwrap();

        assert_eq!(node_id, 42);
        assert_eq!(reg.metadata.cert_pem_chain, vec!["node-42-certificate"]);
    }

    #[test]
    fn legacy_started_response_without_bound_certificate_fails_closed() {
        let mut reg = registration();
        let error = reg
            .install_started(NodeStarted {
                node_id: 42,
                cert_pem_chain: Vec::new(),
                bearer_generation: 0,
                bearer_expires_in_seconds: 300,
            })
            .unwrap_err();

        assert_eq!(error.to_string(), "control_started_certificate_missing");
        assert!(reg.metadata.cert_pem_chain.is_empty());
    }

    #[test]
    fn plain_http_is_loopback_only() {
        let public = Url::parse("http://192.0.2.10:3030/").unwrap();
        let error = Registration::from_response(
            public.clone(),
            registration_response(public.as_str(), TEST_TOKEN),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "control_transport_insecure");
        assert!(!error.to_string().contains(TEST_TOKEN));
    }

    #[test]
    fn forged_and_downgraded_control_urls_fail_closed_without_secret_errors() {
        let control_url = Url::parse("https://control.example/").unwrap();
        let mut forged = registration_response(control_url.as_str(), TEST_TOKEN);
        forged.urls.node_started = "https://attacker.example/started".to_owned();
        let error = Registration::from_response(control_url.clone(), forged).unwrap_err();
        assert_eq!(error.to_string(), "control_endpoint_origin_mismatch");
        assert!(!error.to_string().contains(TEST_TOKEN));

        let mut downgraded = registration_response(control_url.as_str(), TEST_TOKEN);
        downgraded.urls.node_refreshed = "http://control.example/refreshed".to_owned();
        let error = Registration::from_response(control_url, downgraded).unwrap_err();
        assert_eq!(error.to_string(), "control_endpoint_origin_mismatch");
        assert!(!error.to_string().contains(TEST_TOKEN));
    }

    #[test]
    fn every_returned_control_url_is_bound_to_the_registration_origin() {
        let control_url = Url::parse("https://control.example/").unwrap();
        for endpoint in [
            "api_base",
            "nodes",
            "node_started",
            "node_refreshed",
            "node_stopped",
            "get_module",
            "add_module",
            "get_nodes",
        ] {
            let mut response = registration_response(control_url.as_str(), TEST_TOKEN);
            let forged = match endpoint {
                "api_base" => "https://attacker.example/".to_owned(),
                "nodes" | "get_nodes" => "https://attacker.example/nodes".to_owned(),
                "node_started" => "https://attacker.example/started".to_owned(),
                "node_refreshed" => "https://attacker.example/refreshed".to_owned(),
                "node_stopped" => "https://attacker.example/stopped".to_owned(),
                "get_module" => "https://attacker.example/module/{id}".to_owned(),
                "add_module" => "https://attacker.example/module".to_owned(),
                _ => unreachable!(),
            };
            match endpoint {
                "api_base" => response.urls.api_base = forged,
                "nodes" => response.urls.nodes = forged,
                "node_started" => response.urls.node_started = forged,
                "node_refreshed" => response.urls.node_refreshed = forged,
                "node_stopped" => response.urls.node_stopped = forged,
                "get_module" => response.urls.get_module = forged,
                "add_module" => response.urls.add_module = forged,
                "get_nodes" => response.urls.get_nodes = forged,
                _ => unreachable!(),
            }

            let error = Registration::from_response(control_url.clone(), response).unwrap_err();
            assert_eq!(error.to_string(), "control_endpoint_origin_mismatch");
            assert!(!error.to_string().contains(TEST_TOKEN));
        }
    }

    #[test]
    fn runtime_registration_and_authorization_header_are_redacted() {
        let registration = registration();
        let debug = format!("{registration:?}");
        assert!(!debug.contains(TEST_TOKEN));
        assert!(registration.authorization_header().unwrap().is_sensitive());
    }

    #[test]
    fn ipv6_loopback_http_is_accepted() {
        let base = "http://[::1]:3030/";
        Registration::from_response(
            Url::parse(base).unwrap(),
            registration_response(base, TEST_TOKEN),
        )
        .expect("literal IPv6 loopback is an allowed development transport");
    }

    #[test]
    fn an_unacknowledged_rotation_reuses_the_same_pending_secret() {
        let mut registration = registration();
        let first = registration.rotation_request().unwrap();
        let pending = first.next_authentication_token.expose_for_wire().to_owned();
        drop(first);

        let retry = registration.rotation_request().unwrap();
        assert_eq!(retry.next_authentication_token.expose_for_wire(), pending);
        registration
            .install_refreshed(NodeRefreshed {
                next_bearer_generation: 1,
                bearer_expires_in_seconds: 300,
            })
            .unwrap();
        assert_eq!(
            registration.bearer.expose_for_authorization(),
            pending.as_str()
        );
        assert!(registration.pending_bearer.is_none());
        assert_eq!(registration.bearer_generation, 1);
    }

    #[test]
    fn last_public_client_drop_revokes_authority_and_stops_workers() {
        let client = Client::from_static_nodes(registration_metadata(), 1, Vec::new());
        let inner = Arc::clone(&client.inner);
        let stop = inner.worker_stop.subscribe();
        inner
            .reg
            .write()
            .unwrap()
            .rotation_request()
            .expect("pending bearer");

        let sibling = client.clone();
        let worker = Client::worker(Arc::clone(&inner));
        drop(client);
        assert!(!inner.stopped.load(atomic::Ordering::Acquire));
        drop(worker);
        assert!(!inner.stopped.load(atomic::Ordering::Acquire));

        drop(sibling);
        assert!(inner.stopped.load(atomic::Ordering::Acquire));
        assert!(*stop.borrow());
        let registration = inner.reg.read().unwrap();
        assert!(registration.bearer.is_revoked());
        assert!(registration.pending_bearer.is_none());
    }

    #[tokio::test]
    async fn bearer_rotations_are_single_flight() {
        let client = Client::from_static_nodes(registration_metadata(), 1, Vec::new());
        let guard = client.inner.bearer_rotation.lock().await;
        let contender = client.clone();
        let task = tokio::spawn(async move { contender.refresh_bearer().await });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());

        client.inner.stopped.store(true, atomic::Ordering::Release);
        drop(guard);
        assert_eq!(
            task.await.unwrap().unwrap_err().to_string(),
            "control_registration_revoked"
        );
    }
}
