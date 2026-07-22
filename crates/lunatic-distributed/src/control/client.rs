use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use lunatic_control::api::*;
use lunatic_control::NodeInfo;
use lunatic_process::runtimes::RawWasm;
use reqwest::{Client as HttpClient, Url};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{atomic, atomic::AtomicU64, atomic::AtomicUsize, Arc, RwLock},
    time::Duration,
};

pub const DEFAULT_MAX_TOPOLOGY_NODES: usize = 1_024;

#[derive(Clone)]
pub struct Client {
    inner: Arc<InnerClient>,
}

pub struct InnerClient {
    reg: Registration,
    node_id: u64,
    http_client: HttpClient,
    next_message_id: AtomicU64,
    next_query_id: AtomicU64,
    node_queries: DashMap<u64, Vec<u64>>,
    pending_node_queries: AtomicUsize,
    nodes: DashMap<u64, NodeInfo>,
    node_ids: RwLock<Vec<u64>>,
    max_topology_nodes: usize,
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

impl Client {
    /// Construct a client from an already-known static node topology.
    ///
    /// This avoids an HTTP control-plane dependency for embedded deployments and
    /// deterministic node-to-node protocol tests.
    #[doc(hidden)]
    pub fn from_static_nodes(reg: Registration, node_id: u64, nodes: Vec<NodeInfo>) -> Self {
        let mut node_ids = nodes.iter().map(|node| node.id).collect::<Vec<_>>();
        node_ids.sort_unstable();
        node_ids.dedup();
        let node_map = DashMap::new();
        for node in nodes {
            node_map.insert(node.id, node);
        }
        Self {
            inner: Arc::new(InnerClient {
                reg,
                node_id,
                http_client: HttpClient::new(),
                next_message_id: AtomicU64::new(1),
                next_query_id: AtomicU64::new(1),
                node_queries: DashMap::new(),
                pending_node_queries: AtomicUsize::new(0),
                nodes: node_map,
                node_ids: RwLock::new(node_ids),
                max_topology_nodes: DEFAULT_MAX_TOPOLOGY_NODES,
            }),
        }
    }

    pub async fn new(
        http_client: HttpClient,
        reg: Registration,
        node_address: SocketAddr,
        attributes: HashMap<String, String>,
    ) -> Result<Self> {
        Self::new_with_topology_limit(
            http_client,
            reg,
            node_address,
            attributes,
            DEFAULT_MAX_TOPOLOGY_NODES,
        )
        .await
    }

    pub async fn new_with_topology_limit(
        http_client: HttpClient,
        mut reg: Registration,
        node_address: SocketAddr,
        attributes: HashMap<String, String>,
        max_topology_nodes: usize,
    ) -> Result<Self> {
        let started = Self::start(
            &http_client,
            &reg,
            NodeStart {
                node_address,
                attributes,
            },
        )
        .await?;
        let node_id = install_started_certificate(&mut reg, started)?;

        let client = Client {
            inner: Arc::new(InnerClient {
                reg,
                node_id,
                http_client,
                next_message_id: AtomicU64::new(1),
                node_queries: DashMap::new(),
                pending_node_queries: AtomicUsize::new(0),
                next_query_id: AtomicU64::new(1),
                nodes: Default::default(),
                node_ids: Default::default(),
                max_topology_nodes,
            }),
        };

        tokio::task::spawn(refresh_nodes_task(client.clone()));
        client.refresh_nodes().await?;

        Ok(client)
    }

    pub async fn register(
        http_client: &HttpClient,
        control_url: Url,
        node_name: uuid::Uuid,
        csr_pem: String,
    ) -> Result<Registration> {
        let reg = Register { node_name, csr_pem };
        Self::send_registration(http_client, control_url, reg).await
    }

    pub fn reg(&self) -> Registration {
        self.inner.reg.clone()
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
        let resp = client
            .post(url)
            .json(&reg)
            .send()
            .await
            .with_context(|| "Error sending HTTP registration request.")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.with_context(|| {
                format!("Error parsing body as a text. Response not successful: {status}")
            })?;
            Err(anyhow!(
                "HTTP registration request returned an error response: {body}"
            ))
        } else {
            let reg = resp
                .json()
                .await
                .with_context(|| "Error parsing the registration request JSON.")?;

            Ok(reg)
        }
    }

    async fn start(
        client: &HttpClient,
        reg: &Registration,
        start: NodeStart,
    ) -> Result<NodeStarted> {
        let resp: NodeStarted = client
            .post(&reg.urls.node_started)
            .json(&start)
            .bearer_auth(&reg.authentication_token)
            .header(
                "x-lunatic-node-name",
                &reg.node_name.hyphenated().to_string(),
            )
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    pub async fn get<T: DeserializeOwned>(&self, url: &str, query: Option<&str>) -> Result<T> {
        let mut url: Url = url.parse()?;
        url.set_query(query);

        let resp: T = self
            .inner
            .http_client
            .get(url.clone())
            .bearer_auth(&self.inner.reg.authentication_token)
            .header(
                "x-lunatic-node-name",
                &self.inner.reg.node_name.hyphenated().to_string(),
            )
            .send()
            .await
            .with_context(|| format!("Error sending HTTP GET request: {}.", url))?
            .error_for_status()
            .with_context(|| format!("HTTP GET request returned an error response: {}", url))?
            .json()
            .await
            .with_context(|| format!("Error parsing the HTTP GET request JSON: {}", url))?;

        Ok(resp)
    }

    pub async fn post<T: Serialize, R: DeserializeOwned>(&self, url: &str, data: T) -> Result<R> {
        let url: Url = url.parse()?;

        let resp: R = self
            .inner
            .http_client
            .post(url.clone())
            .json(&data)
            .bearer_auth(&self.inner.reg.authentication_token)
            .header(
                "x-lunatic-node-name",
                &self.inner.reg.node_name.hyphenated().to_string(),
            )
            .send()
            .await
            .with_context(|| format!("Error sending HTTP POST request: {}.", url))?
            .error_for_status()
            .with_context(|| format!("HTTP POST request returned an error response: {}", url))?
            .json()
            .await
            .with_context(|| format!("Error parsing the HTTP POST request JSON: {}", url))?;

        Ok(resp)
    }

    pub async fn upload<R: DeserializeOwned>(&self, url: &str, body: Vec<u8>) -> Result<R> {
        let url: Url = url.parse()?;

        let resp: R = self
            .inner
            .http_client
            .post(url.clone())
            .body(body)
            .bearer_auth(&self.inner.reg.authentication_token)
            .header(
                "x-lunatic-node-name",
                &self.inner.reg.node_name.hyphenated().to_string(),
            )
            .send()
            .await
            .with_context(|| format!("Error sending HTTP POST request: {}.", url))?
            .error_for_status()
            .with_context(|| format!("HTTP POST request returned an error response: {}", url))?
            .json()
            .await
            .with_context(|| format!("Error parsing the HTTP POST request JSON: {}", url))?;

        Ok(resp)
    }

    pub async fn refresh_nodes(&self) -> Result<()> {
        let resp: NodesList = self.get(&self.inner.reg.urls.nodes, None).await?;
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

    pub async fn notify_node_stopped(&self) -> Result<()> {
        self.post::<_, ()>(&self.inner.reg.urls.node_stopped, ())
            .await?;
        Ok(())
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
        let resp: NodesList = self
            .get(&self.inner.reg.urls.get_nodes, Some(query))
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
            .urls
            .get_module
            .replace("{id}", &module_id.to_string());
        let query = format!("env_id={environment_id}");
        let resp: ModuleBytes = self.get(&url, Some(&query)).await?;
        Ok(resp.bytes)
    }

    pub async fn add_module(&self, module: Vec<u8>) -> Result<RawWasm> {
        let url = &self.inner.reg.urls.add_module;
        let resp: ModuleId = self.upload(url, module.clone()).await?;
        Ok(RawWasm::new(Some(resp.module_id), module))
    }
}

fn install_started_certificate(reg: &mut Registration, started: NodeStarted) -> Result<u64> {
    anyhow::ensure!(
        !started.cert_pem_chain.is_empty(),
        "Control server did not return a node-ID-bound certificate from /started; upgrade the \
         control plane before starting nodes"
    );
    let node_id = u64::try_from(started.node_id)
        .context("Control server returned a negative node identity from /started")?;
    reg.cert_pem_chain = started.cert_pem_chain;
    Ok(node_id)
}

async fn refresh_nodes_task(client: Client) -> Result<()> {
    loop {
        client.refresh_nodes().await.ok();
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration() -> Registration {
        let url = "http://127.0.0.1:1/".to_string();
        Registration {
            node_name: uuid::Uuid::from_u128(1),
            cert_pem_chain: Vec::new(),
            authentication_token: "test".into(),
            root_cert: String::new(),
            urls: ControlUrls {
                api_base: url.clone(),
                nodes: url.clone(),
                node_started: url.clone(),
                node_stopped: url.clone(),
                get_module: url.clone(),
                add_module: url.clone(),
                get_nodes: url,
            },
            envs: Vec::new(),
            is_privileged: true,
        }
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
            registration(),
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
        let client = Client::from_static_nodes(registration(), 1, vec![node(1, 1001, "one")]);

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
        let node_id = install_started_certificate(
            &mut reg,
            NodeStarted {
                node_id: 42,
                cert_pem_chain: vec!["node-42-certificate".into()],
            },
        )
        .unwrap();

        assert_eq!(node_id, 42);
        assert_eq!(reg.cert_pem_chain, vec!["node-42-certificate"]);
    }

    #[test]
    fn legacy_started_response_without_bound_certificate_fails_closed() {
        let mut reg = registration();
        let error = install_started_certificate(
            &mut reg,
            NodeStarted {
                node_id: 42,
                cert_pem_chain: Vec::new(),
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("upgrade the control plane"));
        assert!(reg.cert_pem_chain.is_empty());
    }
}
