use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use hash_map_id::HashMapId;
use lunatic_common_api::{
    emit_audit_event, AuditAction, AuditEvent, AuditEventV1, AuditReason, AuditResult,
    AuditSubject, AuditTarget, AuditTargetKind,
};
use lunatic_distributed::{
    distributed::{GlobalProcessId, RegistryProcessRegistration},
    DistributedCtx, DistributedProcessState,
};
use lunatic_error_api::{ErrorCtx, ErrorResource};
use lunatic_networking_api::{
    DnsIterator, DnsIteratorQuota, NetworkHandleLease, NetworkHandleQuota, NetworkingCtx,
    TcpConnection, TlsConnection, TlsListener,
};
use lunatic_process::env::{Environment, LunaticEnvironment};
use lunatic_process::runtimes::wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime};
use lunatic_process::state::{mailboxes_with_limits, ConfigResources, ProcessState};
use lunatic_process::{
    config::ProcessConfig,
    resource_migration::{ResourceMigrationSnapshot, ResourceSnapshot, ResourceTransferReport},
    state::{SignalReceiver, SignalSender},
};
use lunatic_process::{mailbox::MessageMailbox, message::Message};
use lunatic_process_api::{ProcessConfigCtx, ProcessCtx};
use lunatic_sqlite_api::{SQLiteConnections, SQLiteCtx, SQLiteGuestAllocators, SQLiteStatements};
use lunatic_stdout_capture::StdoutCapture;
use lunatic_timer_api::{TimerCtx, TimerResources};
use lunatic_wasi_api::{build_wasi_with_audit, LunaticWasiCtx};
use tokio::net::{TcpListener, UdpSocket};
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use wasi_common::WasiCtx;
use wasmtime::{Linker, ResourceLimiter};

use crate::{
    tls_credentials::{
        EphemeralTlsCredentialProvider, TlsCredentialMaterial, TlsCredentialProvider,
        TlsCredentialScope,
    },
    DefaultProcessConfig,
};
use log::warn;

fn bind_tcp_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    catch_unwind(AssertUnwindSafe(|| TcpListener::from_std(listener)))
        .map_err(|_| std::io::Error::other("Tokio I/O driver is unavailable"))?
}

fn bind_udp_socket(addr: SocketAddr) -> std::io::Result<UdpSocket> {
    let socket = std::net::UdpSocket::bind(addr)?;
    socket.set_nonblocking(true)?;
    catch_unwind(AssertUnwindSafe(|| UdpSocket::from_std(socket)))
        .map_err(|_| std::io::Error::other("Tokio I/O driver is unavailable"))?
}

#[derive(Debug, Default)]
pub struct DbResources {
    // sqlite data
    sqlite_connections: SQLiteConnections,
    sqlite_statements: SQLiteStatements,
    sqlite_guest_allocator: SQLiteGuestAllocators,
}

#[derive(Debug)]
struct ResourceCounts {
    open_file_descriptors: u32,
    open_network_connections: u32,
    open_dns_iterators: u32,
    max_file_descriptors: u32,
    max_network_connections: u32,
    max_dns_iterators: u32,
}

#[derive(Debug)]
pub struct ResourceStats {
    counts: Mutex<ResourceCounts>,
}

impl ResourceStats {
    fn new(max_file_descriptors: u32, max_network_connections: u32) -> Self {
        Self {
            counts: Mutex::new(ResourceCounts {
                open_file_descriptors: 0,
                open_network_connections: 0,
                open_dns_iterators: 0,
                max_file_descriptors,
                max_network_connections,
                max_dns_iterators: max_network_connections,
            }),
        }
    }

    fn reserve_file_descriptor(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        if counts.open_file_descriptors >= counts.max_file_descriptors {
            anyhow::bail!(
                "Max file descriptors ({}) reached",
                counts.max_file_descriptors
            );
        }
        counts.open_file_descriptors += 1;
        Ok(())
    }

    fn release_file_descriptor(&self) {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        if counts.open_file_descriptors > 0 {
            counts.open_file_descriptors -= 1;
        }
    }

    fn reserve_network_connection(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        if counts.open_network_connections >= counts.max_network_connections {
            anyhow::bail!(
                "Max network connections ({}) reached",
                counts.max_network_connections
            );
        }
        counts.open_network_connections += 1;
        Ok(())
    }

    fn release_network_connection(&self) {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        if counts.open_network_connections > 0 {
            counts.open_network_connections -= 1;
        }
    }

    fn counts(&self) -> (u32, u32) {
        let counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        (
            counts.open_file_descriptors,
            counts.open_network_connections,
        )
    }

    fn dns_iterator_count(&self) -> u32 {
        self.counts
            .lock()
            .expect("resource accounting mutex poisoned")
            .open_dns_iterators
    }

    fn validate_limits(
        &self,
        max_file_descriptors: u32,
        max_network_connections: u32,
    ) -> Result<()> {
        let counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        anyhow::ensure!(
            counts.open_file_descriptors <= max_file_descriptors,
            "{} open file descriptors exceed replacement limit {}",
            counts.open_file_descriptors,
            max_file_descriptors
        );
        anyhow::ensure!(
            counts.open_network_connections <= max_network_connections,
            "{} open network handles exceed replacement limit {}",
            counts.open_network_connections,
            max_network_connections
        );
        anyhow::ensure!(
            counts.open_dns_iterators <= max_network_connections,
            "{} open DNS iterators exceed replacement limit {}",
            counts.open_dns_iterators,
            max_network_connections
        );
        Ok(())
    }

    fn set_limits(&self, max_file_descriptors: u32, max_network_connections: u32) {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        debug_assert!(counts.open_file_descriptors <= max_file_descriptors);
        debug_assert!(counts.open_network_connections <= max_network_connections);
        debug_assert!(counts.open_dns_iterators <= max_network_connections);
        counts.max_file_descriptors = max_file_descriptors;
        counts.max_network_connections = max_network_connections;
        counts.max_dns_iterators = max_network_connections;
    }
}

impl DnsIteratorQuota for ResourceStats {
    fn reserve(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        if counts.open_dns_iterators >= counts.max_dns_iterators {
            anyhow::bail!("Max DNS iterators ({}) reached", counts.max_dns_iterators);
        }
        counts.open_dns_iterators += 1;
        Ok(())
    }

    fn release(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        if counts.open_dns_iterators == 0 {
            anyhow::bail!("DNS iterator resource accounting underflow");
        }
        counts.open_dns_iterators -= 1;
        Ok(())
    }
}

impl NetworkHandleQuota for ResourceStats {
    fn reserve(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        if counts.open_file_descriptors >= counts.max_file_descriptors {
            anyhow::bail!(
                "Max file descriptors ({}) reached",
                counts.max_file_descriptors
            );
        }
        if counts.open_network_connections >= counts.max_network_connections {
            anyhow::bail!(
                "Max network connections ({}) reached",
                counts.max_network_connections
            );
        }
        counts.open_file_descriptors += 1;
        counts.open_network_connections += 1;
        Ok(())
    }

    fn release(&self) -> Result<()> {
        let mut counts = self
            .counts
            .lock()
            .expect("resource accounting mutex poisoned");
        if counts.open_file_descriptors == 0 || counts.open_network_connections == 0 {
            anyhow::bail!(
                "Network resource accounting underflow (file descriptors={}, network handles={})",
                counts.open_file_descriptors,
                counts.open_network_connections
            );
        }
        counts.open_file_descriptors -= 1;
        counts.open_network_connections -= 1;
        Ok(())
    }
}

pub struct DefaultProcessState {
    // Process id
    pub(crate) id: u64,
    pub(crate) environment: Arc<LunaticEnvironment>,
    pub(crate) distributed: Option<DistributedProcessState>,
    distributed_registry_owner: Option<Arc<RegistryProcessRegistration>>,
    // The WebAssembly runtime
    runtime: Option<WasmtimeRuntime>,
    // The module that this process was spawned from
    module: Option<Arc<WasmtimeCompiledModule<Self>>>,
    // The process configuration
    config: Arc<DefaultProcessConfig>,
    // A space that can be used to temporarily store messages when sending or receiving them.
    // Messages can contain resources that need to be added across multiple host. Likewise,
    // receiving messages is done in two steps, first the message size is returned to allow the
    // guest to reserve enough space, and then it's received. Both of those actions use
    // `message` as a temp space to store messages across host calls.
    message: Option<Message>,
    // Signals sent to the mailbox
    signal_mailbox: (SignalSender, SignalReceiver),
    // Messages sent to the process
    message_mailbox: MessageMailbox,
    // Resources
    resources: Resources,
    // WASI
    wasi: WasiCtx,
    // WASI stdout stream
    wasi_stdout: Option<StdoutCapture>,
    // WASI stderr stream
    wasi_stderr: Option<StdoutCapture>,
    // Set to true if the WASM module has been instantiated
    initialized: bool,
    // database resources
    db_resources: DbResources,
    registry: Arc<RwLock<HashMap<String, (u64, u64)>>>,
    // Resource usage stats (Phase 3)
    resource_stats: Arc<ResourceStats>,
    // Host-owned TLS identities used only by serialized listener restoration.
    tls_credential_provider: Arc<dyn TlsCredentialProvider>,
}

impl DefaultProcessState {
    pub fn can_open_file_descriptor(&mut self) -> anyhow::Result<()> {
        self.resource_stats.reserve_file_descriptor()
    }

    pub fn close_file_descriptor(&mut self) {
        self.resource_stats.release_file_descriptor();
    }

    pub fn can_open_network_connection(&mut self) -> anyhow::Result<()> {
        self.resource_stats.reserve_network_connection()
    }

    pub fn close_network_connection(&mut self) {
        self.resource_stats.release_network_connection();
    }

    /// Atomically reserves one guest-visible network resource handle against
    /// both descriptor and network ceilings.
    pub fn reserve_network_handle(&mut self) -> anyhow::Result<()> {
        NetworkHandleQuota::reserve(self.resource_stats.as_ref())
    }

    /// Releases exactly one guest-visible network resource handle.
    pub fn release_network_handle(&mut self) -> anyhow::Result<()> {
        NetworkHandleQuota::release(self.resource_stats.as_ref())
    }

    pub fn network_resource_counts(&self) -> (u32, u32) {
        self.resource_stats.counts()
    }

    pub fn dns_iterator_count(&self) -> u32 {
        self.resource_stats.dns_iterator_count()
    }

    fn live_network_handle_count(&self) -> usize {
        self.resources.tcp_listeners.len()
            + self.resources.tcp_streams.len()
            + self.resources.tls_listeners.len()
            + self.resources.tls_streams.len()
            + self.resources.udp_sockets.len()
    }

    fn tls_credential_scope(&self) -> TlsCredentialScope {
        TlsCredentialScope::new(self.environment.id(), self.id)
    }

    pub fn new(
        environment: Arc<LunaticEnvironment>,
        distributed: Option<DistributedProcessState>,
        runtime: WasmtimeRuntime,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<DefaultProcessConfig>,
        registry: Arc<RwLock<HashMap<String, (u64, u64)>>>,
    ) -> Result<Self> {
        Self::new_with_tls_credential_provider(
            environment,
            distributed,
            runtime,
            module,
            config,
            registry,
            Arc::new(EphemeralTlsCredentialProvider::default()),
        )
    }

    /// Creates process state with an explicitly managed TLS credential provider.
    ///
    /// The default constructor uses a short-lived process-local provider. A
    /// caller that persists resource snapshots across runtime restarts must
    /// inject a provider that can securely re-provision scoped handles.
    pub fn new_with_tls_credential_provider(
        environment: Arc<LunaticEnvironment>,
        distributed: Option<DistributedProcessState>,
        runtime: WasmtimeRuntime,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<DefaultProcessConfig>,
        registry: Arc<RwLock<HashMap<String, (u64, u64)>>>,
        tls_credential_provider: Arc<dyn TlsCredentialProvider>,
    ) -> Result<Self> {
        config
            .validate_runtime_limits()
            .map_err(anyhow::Error::msg)?;
        let (signal_mailbox, message_mailbox) = mailboxes_with_limits(
            config.get_max_signal_queue() as usize,
            config.get_max_mailbox_messages() as usize,
            config.get_max_message_size(),
            config.get_max_message_resources(),
        );
        let id = environment.get_next_process_id();
        let distributed_registry_owner = distributed.as_ref().map(|distributed| {
            distributed
                .node_client
                .register_process_owner(GlobalProcessId::new(
                    distributed.node_id(),
                    environment.id(),
                    id,
                ))
        });
        let mut audit_subject = AuditSubject::new()
            .with_environment_id(environment.id())
            .with_process_id(id);
        if let Some(distributed) = distributed.as_ref() {
            audit_subject = audit_subject.with_node_id(distributed.node_id());
        }
        let state = Self {
            id,
            environment,
            distributed,
            distributed_registry_owner,
            runtime: Some(runtime),
            module: Some(module),
            config: config.clone(),
            message: None,
            signal_mailbox,
            message_mailbox,
            resources: Resources::default(),
            wasi: build_wasi_with_audit(
                Some(config.command_line_arguments()),
                Some(config.environment_variables()),
                config.preopened_dirs(),
                audit_subject,
            )?,
            wasi_stdout: None,
            wasi_stderr: None,
            initialized: false,
            registry,
            db_resources: DbResources::default(),
            resource_stats: Arc::new(ResourceStats::new(
                config.get_max_file_descriptors(),
                config.get_max_network_connections(),
            )),
            tls_credential_provider,
        };
        Ok(state)
    }
}

impl ProcessState for DefaultProcessState {
    type Config = DefaultProcessConfig;

    fn new_state(
        &self,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<DefaultProcessConfig>,
    ) -> Result<Self> {
        self.config
            .validate_child_config(config.as_ref())
            .map_err(anyhow::Error::msg)?;
        let (signal_mailbox, message_mailbox) = mailboxes_with_limits(
            config.get_max_signal_queue() as usize,
            config.get_max_mailbox_messages() as usize,
            config.get_max_message_size(),
            config.get_max_message_resources(),
        );
        let id = self.environment.get_next_process_id();
        let distributed_registry_owner = self.distributed.as_ref().map(|distributed| {
            distributed
                .node_client
                .register_process_owner(GlobalProcessId::new(
                    distributed.node_id(),
                    self.environment.id(),
                    id,
                ))
        });
        let mut audit_subject = AuditSubject::new()
            .with_environment_id(self.environment.id())
            .with_process_id(id);
        if let Some(distributed) = self.distributed.as_ref() {
            audit_subject = audit_subject.with_node_id(distributed.node_id());
        }
        let state = Self {
            id,
            environment: self.environment.clone(),
            distributed: self.distributed.clone(),
            distributed_registry_owner,
            runtime: self.runtime.clone(),
            module: Some(module),
            config: config.clone(),
            message: None,
            signal_mailbox,
            message_mailbox,
            resources: Resources::default(),
            wasi: build_wasi_with_audit(
                Some(config.command_line_arguments()),
                Some(config.environment_variables()),
                config.preopened_dirs(),
                audit_subject,
            )?,
            wasi_stdout: None,
            wasi_stderr: None,
            initialized: false,
            registry: self.registry.clone(),
            db_resources: DbResources::default(),
            resource_stats: Arc::new(ResourceStats::new(
                config.get_max_file_descriptors(),
                config.get_max_network_connections(),
            )),
            tls_credential_provider: self.tls_credential_provider.clone(),
        };
        Ok(state)
    }

    fn new_state_for_reload(
        &self,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<DefaultProcessConfig>,
    ) -> Result<Self> {
        self.config
            .validate_child_config(config.as_ref())
            .map_err(anyhow::Error::msg)?;

        let mut audit_subject = AuditSubject::new()
            .with_environment_id(self.environment.id())
            .with_process_id(self.id);
        if let Some(distributed) = self.distributed.as_ref() {
            audit_subject = audit_subject.with_node_id(distributed.node_id());
        }

        Ok(Self {
            id: self.id,
            environment: self.environment.clone(),
            distributed: self.distributed.clone(),
            distributed_registry_owner: self.distributed_registry_owner.clone(),
            runtime: self.runtime.clone(),
            module: Some(module),
            config: config.clone(),
            message: None,
            signal_mailbox: self.signal_mailbox.clone(),
            message_mailbox: self.message_mailbox.clone(),
            resources: Resources::default(),
            wasi: build_wasi_with_audit(
                Some(config.command_line_arguments()),
                Some(config.environment_variables()),
                config.preopened_dirs(),
                audit_subject,
            )?,
            wasi_stdout: None,
            wasi_stderr: None,
            initialized: false,
            registry: self.registry.clone(),
            db_resources: DbResources::default(),
            resource_stats: Arc::new(ResourceStats::new(
                config.get_max_file_descriptors(),
                config.get_max_network_connections(),
            )),
            tls_credential_provider: self.tls_credential_provider.clone(),
        })
    }

    fn register(linker: &mut Linker<Self>) -> Result<()> {
        lunatic_error_api::register(linker)?;
        lunatic_process_api::register(linker)?;
        lunatic_messaging_api::register(linker)?;
        lunatic_timer_api::register(linker)?;
        lunatic_networking_api::register(linker)?;
        lunatic_version_api::register(linker)?;
        lunatic_wasi_api::register(linker)?;
        lunatic_wasi_api::register_checked(linker)?;
        lunatic_registry_api::register_distributed::<Self, LunaticEnvironment>(linker)?;
        lunatic_distributed_api::register(linker)?;
        lunatic_sqlite_api::register(linker)?;
        #[cfg(feature = "metrics")]
        lunatic_metrics_api::register(linker)?;
        lunatic_trap_api::register(linker)?;
        Ok(())
    }

    fn initialize(&mut self) {
        self.initialized = true;
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn runtime(&self) -> &WasmtimeRuntime {
        self.runtime.as_ref().unwrap()
    }

    fn config(&self) -> &Arc<DefaultProcessConfig> {
        &self.config
    }

    fn module(&self) -> &Arc<WasmtimeCompiledModule<Self>> {
        self.module.as_ref().unwrap()
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn process_exit_hook(&self) -> Option<Arc<dyn lunatic_process::env::ProcessExitHook>> {
        self.distributed_registry_owner
            .clone()
            .map(|owner| owner as Arc<dyn lunatic_process::env::ProcessExitHook>)
    }

    fn audit_node_id(&self) -> Option<u64> {
        self.distributed
            .as_ref()
            .map(DistributedProcessState::node_id)
    }

    fn audit_environment_id(&self) -> Option<u64> {
        Some(self.environment.id())
    }

    fn signal_mailbox(&self) -> &(SignalSender, SignalReceiver) {
        &self.signal_mailbox
    }

    fn message_mailbox(&self) -> &MessageMailbox {
        &self.message_mailbox
    }

    fn config_resources(&self) -> &ConfigResources<<DefaultProcessState as ProcessState>::Config> {
        &self.resources.configs
    }

    fn config_resources_mut(
        &mut self,
    ) -> &mut ConfigResources<<DefaultProcessState as ProcessState>::Config> {
        &mut self.resources.configs
    }

    fn registry(&self) -> &Arc<RwLock<HashMap<String, (u64, u64)>>> {
        &self.registry
    }

    fn transfer_runtime_resources_to(
        &mut self,
        target: &mut Self,
    ) -> Result<ResourceTransferReport> {
        let source_network_handles = self.live_network_handle_count();
        let (source_file_descriptors, source_network_connections) = self.resource_stats.counts();
        let source_dns_iterators = self.resource_stats.dns_iterator_count();
        anyhow::ensure!(
            source_file_descriptors as usize >= source_network_handles
                && source_network_connections as usize >= source_network_handles,
            "source network accounting drifted (resources={}, file descriptors={}, network handles={})",
            source_network_handles,
            source_file_descriptors,
            source_network_connections,
        );
        anyhow::ensure!(
            source_dns_iterators as usize == self.resources.dns_iterators.len(),
            "source DNS iterator accounting drifted (resources={}, reservations={})",
            self.resources.dns_iterators.len(),
            source_dns_iterators,
        );
        let target_counts = target.resource_stats.counts();
        let target_dns_iterators = target.resource_stats.dns_iterator_count();
        anyhow::ensure!(
            target.resources.tcp_listeners.is_empty()
                && target.resources.tcp_streams.is_empty()
                && target.resources.tls_listeners.is_empty()
                && target.resources.tls_streams.is_empty()
                && target.resources.udp_sockets.is_empty()
                && target.resources.dns_iterators.is_empty()
                && target_counts == (0, 0)
                && target_dns_iterators == 0,
            "replacement process state already owns network resources"
        );
        self.resource_stats.validate_limits(
            target.config.get_max_file_descriptors(),
            target.config.get_max_network_connections(),
        )?;

        let report = ResourceTransferReport {
            tcp_listeners: self.resources.tcp_listeners.len(),
            tcp_streams: self.resources.tcp_streams.len(),
            tls_listeners: self.resources.tls_listeners.len(),
            tls_streams: self.resources.tls_streams.len(),
            udp_sockets: self.resources.udp_sockets.len(),
            dns_iterators: self.resources.dns_iterators.len(),
        };

        // Swap the complete host-owned state only after every fallible check has
        // succeeded. This preserves resource IDs, live network sessions,
        // configuration/module handles, timers, errors, SQLite handles, WASI
        // descriptors and message scratch state as one commit operation. The
        // old instance receives the replacement's fresh state and can then be
        // dropped without closing resources now owned by the new instance.
        self.resource_stats.set_limits(
            target.config.get_max_file_descriptors(),
            target.config.get_max_network_connections(),
        );
        std::mem::swap(&mut self.resources, &mut target.resources);
        std::mem::swap(&mut self.db_resources, &mut target.db_resources);
        std::mem::swap(&mut self.wasi, &mut target.wasi);
        std::mem::swap(&mut self.wasi_stdout, &mut target.wasi_stdout);
        std::mem::swap(&mut self.wasi_stderr, &mut target.wasi_stderr);
        std::mem::swap(&mut self.message, &mut target.message);
        std::mem::swap(&mut self.resource_stats, &mut target.resource_stats);

        Ok(report)
    }

    fn capture_resource_snapshot(&self) -> Result<Option<ResourceMigrationSnapshot>> {
        let mut snapshot = ResourceMigrationSnapshot::new();

        for (id, listener) in self.resources.tcp_listeners.iter() {
            match listener.local_addr() {
                Ok(addr) => snapshot.add_tcp_listener(
                    *id,
                    ResourceSnapshot::TcpListener {
                        local_addr: addr.to_string(),
                    },
                ),
                Err(err) => snapshot.add_tcp_listener(
                    *id,
                    ResourceSnapshot::NonMigratable {
                        resource_type: "tcp_listener".into(),
                        reason: format!("local_addr_failed: {err}"),
                    },
                ),
            }
        }

        for (id, stream) in self.resources.tcp_streams.iter() {
            let reason = match stream.peer_addr() {
                Some(addr) => format!("active peer {addr}"),
                None => "untracked peer address".into(),
            };
            snapshot.add_tcp_stream(
                *id,
                ResourceSnapshot::NonMigratable {
                    resource_type: "tcp_stream".into(),
                    reason,
                },
            );
        }

        for (id, listener) in self.resources.tls_listeners.iter() {
            match listener.listener.local_addr() {
                Ok(addr) => {
                    let credential_handle = self
                        .tls_credential_provider
                        .provision(
                            self.tls_credential_scope(),
                            TlsCredentialMaterial::new(listener.acceptor.clone()),
                        )
                        .map_err(anyhow::Error::new)?;
                    snapshot.add_tls_listener(
                        *id,
                        ResourceSnapshot::TlsListener {
                            local_addr: addr.to_string(),
                            credential_handle,
                        },
                    );
                }
                Err(err) => snapshot.add_tls_listener(
                    *id,
                    ResourceSnapshot::NonMigratable {
                        resource_type: "tls_listener".into(),
                        reason: format!("local_addr_failed: {err}"),
                    },
                ),
            }
        }

        for (id, stream) in self.resources.tls_streams.iter() {
            if let Some(client_metadata) = &stream.client_metadata {
                // Serialized snapshots retain descriptive metadata only. The
                // live stream is transferred directly for in-process reloads.
                let read_timeout = stream.read_timeout.try_lock().ok().and_then(|t| *t);
                let write_timeout = stream.write_timeout.try_lock().ok().and_then(|t| *t);

                snapshot.add_tls_stream(
                    *id,
                    ResourceSnapshot::TlsClientConnectionMetadata {
                        server_name: client_metadata.server_name.clone(),
                        port: client_metadata.port,
                        peer_addr: client_metadata.peer_addr.map(|a| a.to_string()),
                        local_addr: client_metadata.local_addr.map(|a| a.to_string()),
                        custom_root_certs: client_metadata.custom_root_certs.clone(),
                        read_timeout_ms: read_timeout.map(|d| d.as_millis() as u64),
                        write_timeout_ms: write_timeout.map(|d| d.as_millis() as u64),
                    },
                );
            } else {
                // A server-accepted stream cannot be recreated from metadata.
                snapshot.add_tls_stream(
                    *id,
                    ResourceSnapshot::TlsServerConnectionMetadata {
                        requires_peer_reconnect: true,
                        reason: "Serialized snapshots cannot restore server-accepted TLS streams"
                            .into(),
                    },
                );
            }
        }

        for (id, socket) in self.resources.udp_sockets.iter() {
            match socket.local_addr() {
                Ok(addr) => snapshot.add_udp_socket(
                    *id,
                    ResourceSnapshot::UdpSocket {
                        local_addr: addr.to_string(),
                    },
                ),
                Err(err) => snapshot.add_udp_socket(
                    *id,
                    ResourceSnapshot::NonMigratable {
                        resource_type: "udp_socket".into(),
                        reason: format!("local_addr_failed: {err}"),
                    },
                ),
            }
        }

        if snapshot.is_empty() {
            Ok(None)
        } else {
            Ok(Some(snapshot))
        }
    }

    /// Restore runtime resources from a serialized snapshot.
    ///
    /// Serialized TLS stream entries are metadata-only and return an explicit
    /// error. In-process hot reload uses `transfer_runtime_resources_to`
    /// instead, preserving the live TLS session and guest resource ID.
    fn restore_resource_snapshot(&mut self, snapshot: ResourceMigrationSnapshot) -> Result<()> {
        if snapshot.is_empty() {
            return Ok(());
        }

        if let Some((id, entry)) = snapshot.tls_streams.iter().next() {
            match entry {
                ResourceSnapshot::TlsClientConnectionMetadata { .. } => anyhow::bail!(
                    "serialized TLS client stream restoration is unsupported \
                     (resource {id}, endpoint redacted); metadata \
                     cannot recreate the original TLS/application byte stream"
                ),
                ResourceSnapshot::TlsServerConnectionMetadata { .. } => anyhow::bail!(
                    "serialized TLS server stream restoration is unsupported \
                     (resource {id}): reason redacted"
                ),
                _ => anyhow::bail!(
                    "serialized TLS stream restoration is unsupported \
                     (resource {id}, snapshot type redacted)"
                ),
            }
        }

        let (_, target_network_connections) = self.resource_stats.counts();
        anyhow::ensure!(
            self.live_network_handle_count() == 0 && target_network_connections == 0,
            "resource snapshot restore target already owns network resources"
        );

        Handle::try_current().map_err(|_| {
            anyhow::anyhow!("resource restoration requires an active Tokio runtime")
        })?;

        let ResourceMigrationSnapshot {
            tcp_listeners,
            tcp_streams,
            tls_listeners,
            tls_streams,
            udp_sockets,
        } = snapshot;

        let mut restored = 0usize;

        // Resolve, authorize, and bind every TLS listener before mutating any
        // resource table. The RAII leases and bound sockets in this temporary
        // vector are discarded if any later TLS preflight step fails.
        let mut prepared_tls_listeners: Vec<(TlsListener, NetworkHandleLease)> =
            Vec::with_capacity(tls_listeners.len());
        for (_id, entry) in tls_listeners.into_iter() {
            let ResourceSnapshot::TlsListener {
                local_addr,
                credential_handle,
            } = entry
            else {
                anyhow::bail!("TLS listener snapshot entry is invalid");
            };
            let addr = local_addr
                .parse::<SocketAddr>()
                .map_err(|_| anyhow::anyhow!("TLS listener snapshot address is invalid"))?;
            let material = self
                .tls_credential_provider
                .take(self.tls_credential_scope(), &credential_handle)
                .map_err(anyhow::Error::new)?;
            let lease = self
                .reserve_network_handle_lease()
                .map_err(|_| anyhow::anyhow!("TLS listener restore exceeds network limits"))?;
            let listener = bind_tcp_listener(addr)
                .map_err(|_| anyhow::anyhow!("TLS listener rebind failed"))?;
            prepared_tls_listeners.push((
                TlsListener {
                    listener,
                    acceptor: material.into_acceptor(),
                },
                lease,
            ));
        }

        for (_id, entry) in tcp_listeners.into_iter() {
            match entry {
                ResourceSnapshot::TcpListener { local_addr } => {
                    match local_addr.parse::<SocketAddr>() {
                        Ok(addr) => match self.reserve_network_handle_lease() {
                            Ok(lease) => match bind_tcp_listener(addr) {
                                Ok(listener) => {
                                    self.resources.tcp_listeners.add(listener);
                                    lease.into_table_reservation();
                                    restored += 1;
                                }
                                Err(err) => warn!(
                                    "Failed to rebind TCP listener during hot reload: {}",
                                    err
                                ),
                            },
                            Err(err) => {
                                warn!("TCP listener exceeds restored network limits: {}", err)
                            }
                        },
                        Err(err) => warn!("Invalid TCP listener address in snapshot: {}", err),
                    }
                }
                _ => warn!("Unexpected snapshot entry for TCP listener ignored"),
            }
        }

        for (_id, entry) in udp_sockets.into_iter() {
            match entry {
                ResourceSnapshot::UdpSocket { local_addr } => {
                    match local_addr.parse::<SocketAddr>() {
                        Ok(addr) => match self.reserve_network_handle_lease() {
                            Ok(lease) => match bind_udp_socket(addr) {
                                Ok(socket) => {
                                    self.resources.udp_sockets.add(Arc::new(socket));
                                    lease.into_table_reservation();
                                    restored += 1;
                                }
                                Err(err) => {
                                    warn!("Failed to rebind UDP socket during hot reload: {}", err)
                                }
                            },
                            Err(err) => {
                                warn!("UDP socket exceeds restored network limits: {}", err)
                            }
                        },
                        Err(err) => warn!("Invalid UDP socket address in snapshot: {}", err),
                    }
                }
                _ => warn!("Unexpected snapshot entry for UDP socket ignored"),
            }
        }

        for (listener, lease) in prepared_tls_listeners {
            self.resources.tls_listeners.add(listener);
            lease.into_table_reservation();
            restored += 1;
        }

        debug_assert!(tls_streams.is_empty());

        if !tcp_streams.is_empty() {
            warn!(
                "{} TCP stream(s) skipped during hot reload; active connections are not yet migratable",
                tcp_streams.len()
            );
        }

        if restored > 0 {
            log::info!(
                "Restored {} network listener(s) from hot reload snapshot",
                restored
            );
        }

        Ok(())
    }
}

impl Debug for DefaultProcessState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("process", &self.resources)
            .finish()
    }
}

// Limit the maximum memory of the process depending on the environment it was spawned in.
impl ResourceLimiter for DefaultProcessState {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let allowed = desired <= self.config().get_max_memory();
        if !allowed {
            let mut subject = AuditSubject::new()
                .with_environment_id(self.environment.id())
                .with_process_id(self.id);
            if let Some(distributed) = self.distributed.as_ref() {
                subject = subject.with_node_id(distributed.node_id());
            }
            emit_audit_event(AuditEventV1::new(
                AuditEvent::ResourceLimitDenied,
                AuditAction::Grow,
                AuditResult::Denied,
                AuditReason::ResourceLimit,
                subject,
                AuditTarget::new(AuditTargetKind::Memory),
            ));
        }
        Ok(allowed)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let allowed = desired <= self.config().get_max_table_elements() as usize;
        if !allowed {
            let mut subject = AuditSubject::new()
                .with_environment_id(self.environment.id())
                .with_process_id(self.id);
            if let Some(distributed) = self.distributed.as_ref() {
                subject = subject.with_node_id(distributed.node_id());
            }
            emit_audit_event(AuditEventV1::new(
                AuditEvent::ResourceLimitDenied,
                AuditAction::Grow,
                AuditResult::Denied,
                AuditReason::ResourceLimit,
                subject,
                AuditTarget::new(AuditTargetKind::Table),
            ));
        }
        Ok(allowed)
    }

    // Allow one instance per store
    fn instances(&self) -> usize {
        1
    }

    // Allow one table per store
    fn tables(&self) -> usize {
        1
    }

    // Allow one memory per store
    fn memories(&self) -> usize {
        1
    }
}

impl ErrorCtx for DefaultProcessState {
    fn error_resources(&self) -> &ErrorResource {
        &self.resources.errors
    }

    fn error_resources_mut(&mut self) -> &mut ErrorResource {
        &mut self.resources.errors
    }
}

impl ProcessCtx<DefaultProcessState> for DefaultProcessState {
    fn mailbox(&mut self) -> &mut MessageMailbox {
        &mut self.message_mailbox
    }

    fn message_scratch_area(&mut self) -> &mut Option<Message> {
        &mut self.message
    }

    fn module_resources(&self) -> &lunatic_process_api::ModuleResources<DefaultProcessState> {
        &self.resources.modules
    }

    fn module_resources_mut(
        &mut self,
    ) -> &mut lunatic_process_api::ModuleResources<DefaultProcessState> {
        &mut self.resources.modules
    }

    fn environment(&self) -> Arc<dyn Environment> {
        self.environment.clone()
    }
}

impl NetworkingCtx for DefaultProcessState {
    fn tcp_listener_resources(&self) -> &lunatic_networking_api::TcpListenerResources {
        &self.resources.tcp_listeners
    }

    fn tcp_listener_resources_mut(&mut self) -> &mut lunatic_networking_api::TcpListenerResources {
        &mut self.resources.tcp_listeners
    }

    fn tcp_stream_resources(&self) -> &lunatic_networking_api::TcpStreamResources {
        &self.resources.tcp_streams
    }

    fn tcp_stream_resources_mut(&mut self) -> &mut lunatic_networking_api::TcpStreamResources {
        &mut self.resources.tcp_streams
    }

    fn tls_listener_resources(&self) -> &lunatic_networking_api::TlsListenerResources {
        &self.resources.tls_listeners
    }

    fn tls_listener_resources_mut(&mut self) -> &mut lunatic_networking_api::TlsListenerResources {
        &mut self.resources.tls_listeners
    }

    fn tls_stream_resources(&self) -> &lunatic_networking_api::TlsStreamResources {
        &self.resources.tls_streams
    }

    fn tls_stream_resources_mut(&mut self) -> &mut lunatic_networking_api::TlsStreamResources {
        &mut self.resources.tls_streams
    }

    fn udp_resources(&self) -> &lunatic_networking_api::UdpResources {
        &self.resources.udp_sockets
    }

    fn udp_resources_mut(&mut self) -> &mut lunatic_networking_api::UdpResources {
        &mut self.resources.udp_sockets
    }

    fn dns_resources(&self) -> &lunatic_networking_api::DnsResources {
        &self.resources.dns_iterators
    }

    fn dns_resources_mut(&mut self) -> &mut lunatic_networking_api::DnsResources {
        &mut self.resources.dns_iterators
    }

    fn audit_node_id(&self) -> Option<u64> {
        self.distributed
            .as_ref()
            .map(DistributedProcessState::node_id)
    }

    fn audit_environment_id(&self) -> Option<u64> {
        Some(self.environment.id())
    }

    fn audit_process_id(&self) -> Option<u64> {
        Some(self.id)
    }

    fn network_handle_quota(&self) -> Option<Arc<dyn NetworkHandleQuota>> {
        Some(self.resource_stats.clone())
    }

    fn dns_iterator_quota(&self) -> Option<Arc<dyn DnsIteratorQuota>> {
        Some(self.resource_stats.clone())
    }

    fn can_open_network_connection(&mut self) -> anyhow::Result<()> {
        Self::can_open_network_connection(self)
    }

    fn close_network_connection(&mut self) {
        Self::close_network_connection(self)
    }

    fn reserve_network_handle(&mut self) -> anyhow::Result<()> {
        Self::reserve_network_handle(self)
    }

    fn release_network_handle(&mut self) -> anyhow::Result<()> {
        Self::release_network_handle(self)
    }
}

impl TimerCtx for DefaultProcessState {
    fn timer_resources(&self) -> &TimerResources {
        &self.resources.timers
    }

    fn timer_resources_mut(&mut self) -> &mut TimerResources {
        &mut self.resources.timers
    }
}

impl LunaticWasiCtx for DefaultProcessState {
    fn wasi(&self) -> &WasiCtx {
        &self.wasi
    }

    fn wasi_mut(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }

    // Redirect the stdout stream
    fn set_stdout(&mut self, stdout: StdoutCapture) {
        self.wasi_stdout = Some(stdout.clone());
        self.wasi.set_stdout(Box::new(stdout));
    }

    // Redirect the stderr stream
    fn set_stderr(&mut self, stderr: StdoutCapture) {
        self.wasi_stderr = Some(stderr.clone());
        self.wasi.set_stderr(Box::new(stderr));
    }

    fn get_stdout(&self) -> Option<&StdoutCapture> {
        self.wasi_stdout.as_ref()
    }

    fn get_stderr(&self) -> Option<&StdoutCapture> {
        self.wasi_stderr.as_ref()
    }
}

impl SQLiteCtx for DefaultProcessState {
    fn sqlite_connections(&self) -> &SQLiteConnections {
        &self.db_resources.sqlite_connections
    }

    fn sqlite_connections_mut(&mut self) -> &mut SQLiteConnections {
        &mut self.db_resources.sqlite_connections
    }

    fn sqlite_statements_mut(&mut self) -> &mut SQLiteStatements {
        &mut self.db_resources.sqlite_statements
    }

    fn sqlite_statements(&self) -> &SQLiteStatements {
        &self.db_resources.sqlite_statements
    }

    fn sqlite_guest_allocator(&self) -> &SQLiteGuestAllocators {
        &self.db_resources.sqlite_guest_allocator
    }
    fn sqlite_guest_allocator_mut(&mut self) -> &mut SQLiteGuestAllocators {
        &mut self.db_resources.sqlite_guest_allocator
    }
}

#[derive(Default, Debug)]
pub(crate) struct Resources {
    pub(crate) configs: HashMapId<DefaultProcessConfig>,
    pub(crate) modules: HashMapId<Arc<WasmtimeCompiledModule<DefaultProcessState>>>,
    pub(crate) timers: TimerResources,
    pub(crate) dns_iterators: HashMapId<DnsIterator>,
    pub(crate) tcp_listeners: HashMapId<TcpListener>,
    pub(crate) tcp_streams: HashMapId<Arc<TcpConnection>>,
    pub(crate) tls_listeners: HashMapId<TlsListener>,
    pub(crate) tls_streams: HashMapId<Arc<TlsConnection>>,
    pub(crate) udp_sockets: HashMapId<Arc<UdpSocket>>,
    pub(crate) errors: HashMapId<anyhow::Error>,
}

impl DistributedCtx<LunaticEnvironment> for DefaultProcessState {
    fn distributed_mut(&mut self) -> Result<&mut DistributedProcessState> {
        match self.distributed.as_mut() {
            Some(d) => Ok(d),
            None => Err(anyhow::anyhow!("Distributed is not initialized")),
        }
    }

    fn distributed(&self) -> Result<&DistributedProcessState> {
        match self.distributed.as_ref() {
            Some(d) => Ok(d),
            None => Err(anyhow::anyhow!("Distributed is not initialized")),
        }
    }

    fn module_id(&self) -> u64 {
        self.module
            .as_ref()
            .and_then(|m| m.source().id)
            .unwrap_or(0)
    }

    fn environment_id(&self) -> u64 {
        self.environment.id()
    }

    fn can_spawn(&self) -> bool {
        self.config().can_spawn_processes()
    }

    fn new_dist_state(
        environment: Arc<LunaticEnvironment>,
        distributed: DistributedProcessState,
        runtime: WasmtimeRuntime,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<Self::Config>,
    ) -> Result<Self> {
        config
            .validate_distributed_config()
            .map_err(anyhow::Error::msg)?;
        let (signal_mailbox, message_mailbox) = mailboxes_with_limits(
            config.get_max_signal_queue() as usize,
            config.get_max_mailbox_messages() as usize,
            config.get_max_message_size(),
            config.get_max_message_resources(),
        );
        let id = environment.get_next_process_id();
        let distributed_registry_owner = Some(distributed.node_client.register_process_owner(
            GlobalProcessId::new(distributed.node_id(), environment.id(), id),
        ));
        let audit_subject = AuditSubject::new()
            .with_node_id(distributed.node_id())
            .with_environment_id(environment.id())
            .with_process_id(id);
        let state = Self {
            id,
            environment,
            distributed: Some(distributed),
            distributed_registry_owner,
            runtime: Some(runtime),
            module: Some(module),
            config: config.clone(),
            message: None,
            signal_mailbox,
            message_mailbox,
            resources: Resources::default(),
            wasi: build_wasi_with_audit(
                Some(config.command_line_arguments()),
                Some(config.environment_variables()),
                config.preopened_dirs(),
                audit_subject,
            )?,
            wasi_stdout: None,
            wasi_stderr: None,
            initialized: false,
            registry: Default::default(), // TODO move registry into env?
            db_resources: DbResources::default(),
            resource_stats: Arc::new(ResourceStats::new(
                config.get_max_file_descriptors(),
                config.get_max_network_connections(),
            )),
            tls_credential_provider: Arc::new(EphemeralTlsCredentialProvider::default()),
        };
        Ok(state)
    }
}

impl lunatic_process::reloadable_state::ReloadableState for DefaultProcessState {
    fn serialize_state(&self) -> anyhow::Result<Vec<u8>> {
        let message_count = self.message_mailbox.len() as u64;

        let mut result = Vec::new();
        result.extend_from_slice(&self.id.to_le_bytes());
        result.extend_from_slice(&message_count.to_le_bytes());

        Ok(result)
    }

    fn deserialize_state(_bytes: &[u8]) -> anyhow::Result<Self> {
        Err(anyhow::anyhow!(
            "DefaultProcessState deserialization is handled by hot reload infrastructure, not directly callable"
        ))
    }

    fn code_change(&mut self, old_version: u32, new_version: u32) -> anyhow::Result<()> {
        log::info!(
            "DefaultProcessState code_change: {} -> {} for process {}",
            old_version,
            new_version,
            self.id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        convert::TryFrom,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use crate::tls_credentials::{
        EphemeralTlsCredentialProvider, TlsCredentialMaterial, TlsCredentialProvider,
        TlsCredentialProviderError, TlsCredentialScope,
    };
    use lunatic_process::{
        resource_migration::{ResourceMigrationSnapshot, ResourceSnapshot, TlsCredentialHandle},
        runtimes::wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime},
    };
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    fn test_state_with_tls_provider(
        provider: Arc<dyn TlsCredentialProvider>,
    ) -> anyhow::Result<(
        super::DefaultProcessState,
        Arc<WasmtimeCompiledModule<super::DefaultProcessState>>,
        Arc<crate::DefaultProcessConfig>,
    )> {
        use lunatic_process::env::LunaticEnvironment;
        use tokio::sync::RwLock;

        let runtime = WasmtimeRuntime::new(&lunatic_process::runtimes::wasmtime::default_config())?;
        let module =
            Arc::new(runtime.compile_module(
                wat::parse_str(r#"(module (memory (export "memory") 1))"#)?.into(),
            )?);
        let mut config = crate::DefaultProcessConfig::default();
        config.set_max_file_descriptors(8);
        config.set_max_network_connections(8);
        let config = Arc::new(config);
        let state = super::DefaultProcessState::new_with_tls_credential_provider(
            Arc::new(LunaticEnvironment::new(73)),
            None,
            runtime,
            module.clone(),
            config.clone(),
            Arc::new(RwLock::new(HashMap::new())),
            provider,
        )?;
        Ok((state, module, config))
    }

    fn test_tls_identity() -> anyhow::Result<(TlsAcceptor, TlsConnector, Vec<u8>, Vec<u8>)> {
        use lunatic_distributed::{control::cert, distributed::server::gen_node_cert};
        use tokio_rustls::rustls::{
            pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
            ClientConfig, RootCertStore, ServerConfig,
        };

        let root = cert::test_root_cert()?;
        let server_cert = gen_node_cert("localhost")?;
        let server_cert_pem = server_cert.serialize_pem_with_signer(&root)?;
        let server_key_pem = server_cert.serialize_private_key_pem();
        let server_cert_der = CertificateDer::from_pem_slice(server_cert_pem.as_bytes())?;
        let server_key_der = PrivateKeyDer::from_pem_slice(server_key_pem.as_bytes())?;
        let key_der = server_key_der.secret_der().to_vec();
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![server_cert_der], server_key_der)?;

        let mut root_store = RootCertStore::empty();
        root_store.add(CertificateDer::from_pem_slice(
            root.certificate_pem().as_bytes(),
        )?)?;
        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok((
            TlsAcceptor::from(Arc::new(server_config)),
            TlsConnector::from(Arc::new(client_config)),
            key_der,
            server_key_pem.into_bytes(),
        ))
    }

    struct FailingTlsCredentialProvider;

    impl TlsCredentialProvider for FailingTlsCredentialProvider {
        fn provision(
            &self,
            _scope: TlsCredentialScope,
            _material: TlsCredentialMaterial,
        ) -> Result<TlsCredentialHandle, TlsCredentialProviderError> {
            Err(TlsCredentialProviderError::ProviderFailure)
        }

        fn take(
            &self,
            _scope: TlsCredentialScope,
            _handle: &TlsCredentialHandle,
        ) -> Result<TlsCredentialMaterial, TlsCredentialProviderError> {
            Err(TlsCredentialProviderError::ProviderFailure)
        }
    }

    struct FailAfterOneTlsCredentialProvider {
        material: Mutex<Option<TlsCredentialMaterial>>,
    }

    impl TlsCredentialProvider for FailAfterOneTlsCredentialProvider {
        fn provision(
            &self,
            _scope: TlsCredentialScope,
            _material: TlsCredentialMaterial,
        ) -> Result<TlsCredentialHandle, TlsCredentialProviderError> {
            Err(TlsCredentialProviderError::ProviderFailure)
        }

        fn take(
            &self,
            _scope: TlsCredentialScope,
            _handle: &TlsCredentialHandle,
        ) -> Result<TlsCredentialMaterial, TlsCredentialProviderError> {
            self.material
                .lock()
                .map_err(|_| TlsCredentialProviderError::ProviderFailure)?
                .take()
                .ok_or(TlsCredentialProviderError::ProviderFailure)
        }
    }

    #[tokio::test]
    async fn import_filter_signature_matches() {
        use tokio::sync::RwLock;

        use crate::state::DefaultProcessState;
        use crate::DefaultProcessConfig;
        use lunatic_process::env::Environment;
        use lunatic_process::runtimes::wasmtime::WasmtimeRuntime;
        use lunatic_process::wasm::spawn_wasm;

        // The process-state linker registers both the "lunatic::*" and "wasi_*" namespaces.
        let config = DefaultProcessConfig::default();

        // Create wasmtime runtime
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.consume_fuel(true);
        let runtime = WasmtimeRuntime::new(&wasmtime_config).unwrap();

        let raw_module = wat::parse_file("./wat/all_imports.wat").unwrap();
        let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
        let env = Arc::new(lunatic_process::env::LunaticEnvironment::new(0));
        let registry = Arc::new(RwLock::new(HashMap::new()));
        let state = DefaultProcessState::new(
            env.clone(),
            None,
            runtime.clone(),
            module.clone(),
            Arc::new(config),
            registry,
        )
        .unwrap();

        env.can_spawn_next_process().await.unwrap();

        spawn_wasm(env, runtime, &module, state, "hello", Vec::new(), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn initial_state_rejects_zero_signal_capacity_without_panicking() {
        use tokio::sync::RwLock;

        use crate::{state::DefaultProcessState, DefaultProcessConfig};
        use lunatic_process::{env::LunaticEnvironment, runtimes::wasmtime::WasmtimeRuntime};

        let runtime =
            WasmtimeRuntime::new(&lunatic_process::runtimes::wasmtime::default_config()).unwrap();
        let module = Arc::new(
            runtime
                .compile_module(
                    wat::parse_str(r#"(module (memory (export "memory") 1))"#)
                        .unwrap()
                        .into(),
                )
                .unwrap(),
        );
        let mut config = DefaultProcessConfig::default();
        config.set_max_signal_queue(0);
        let result = DefaultProcessState::new(
            Arc::new(LunaticEnvironment::new(0)),
            None,
            runtime,
            module,
            Arc::new(config),
            Arc::new(RwLock::new(HashMap::new())),
        );

        let error = match result {
            Ok(_) => panic!("zero signal capacity unexpectedly created a process state"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("max_signal_queue"));
    }

    #[tokio::test]
    async fn wasm_process_quota_is_checked_before_store_instantiation() {
        use tokio::sync::RwLock;

        use crate::state::DefaultProcessState;
        use crate::DefaultProcessConfig;
        use lunatic_process::env::{Environment, LunaticEnvironment};
        use lunatic_process::runtimes::wasmtime::WasmtimeRuntime;
        use lunatic_process::wasm::spawn_wasm;

        let runtime =
            WasmtimeRuntime::new(&lunatic_process::runtimes::wasmtime::default_config()).unwrap();
        // Instantiation would trap if quota admission happened too late.
        let raw_module = wat::parse_str(
            r#"(module
                (func $start unreachable)
                (start $start)
                (func (export "run")))"#,
        )
        .unwrap();
        let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
        let environment = Arc::new(LunaticEnvironment::with_max_processes(0, 0));
        let state = DefaultProcessState::new(
            environment.clone(),
            None,
            runtime.clone(),
            module.clone(),
            Arc::new(DefaultProcessConfig::default()),
            Arc::new(RwLock::new(HashMap::new())),
        )
        .unwrap();

        let error = match spawn_wasm(
            environment.clone(),
            runtime,
            &module,
            state,
            "run",
            Vec::new(),
            None,
        )
        .await
        {
            Ok(_) => panic!("quota-zero environment unexpectedly instantiated a Wasm process"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("process limit 0 reached"));
        assert_eq!(environment.process_count(), 0);
    }

    #[tokio::test]
    async fn hot_reload_preserves_dns_iterator_quota_and_releases_on_drop() -> anyhow::Result<()> {
        use tokio::sync::RwLock;

        use lunatic_networking_api::{DnsIterator, DnsIteratorLease, NetworkingCtx};
        use lunatic_process::{
            env::LunaticEnvironment, runtimes::wasmtime::WasmtimeRuntime, state::ProcessState,
        };

        let runtime = WasmtimeRuntime::new(&lunatic_process::runtimes::wasmtime::default_config())?;
        let module =
            Arc::new(runtime.compile_module(
                wat::parse_str(r#"(module (memory (export "memory") 1))"#)?.into(),
            )?);
        let mut config = crate::DefaultProcessConfig::default();
        config.set_max_network_connections(1);
        let config = Arc::new(config);
        let mut old_state = super::DefaultProcessState::new(
            Arc::new(LunaticEnvironment::new(0)),
            None,
            runtime,
            module.clone(),
            config.clone(),
            Arc::new(RwLock::new(HashMap::new())),
        )?;
        let source_quota = old_state
            .dns_iterator_quota()
            .expect("runtime state exposes a stable DNS iterator quota");
        let lease = DnsIteratorLease::reserve_new(source_quota.clone())?;
        let iterator_id = old_state
            .resources
            .dns_iterators
            .add(DnsIterator::with_lease(Vec::new().into_iter(), lease));
        assert_eq!(old_state.dns_iterator_count(), 1);
        assert!(DnsIteratorLease::reserve_new(source_quota.clone()).is_err());

        let mut replacement = old_state.new_state_for_reload(module, config)?;
        let report = old_state.transfer_runtime_resources_to(&mut replacement)?;
        assert_eq!(report.dns_iterators, 1);
        assert_eq!(old_state.dns_iterator_count(), 0);
        assert_eq!(replacement.dns_iterator_count(), 1);
        let replacement_quota = replacement
            .dns_iterator_quota()
            .expect("replacement keeps the transferred DNS quota owner");
        assert!(Arc::ptr_eq(&source_quota, &replacement_quota));

        drop(
            replacement
                .resources
                .dns_iterators
                .remove(iterator_id)
                .expect("transferred DNS iterator ID is preserved"),
        );
        assert_eq!(replacement.dns_iterator_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn hot_reload_transfers_live_tls_listener_without_provider_lookup() -> anyhow::Result<()>
    {
        use lunatic_networking_api::TlsListener;
        use lunatic_process::state::ProcessState;
        use tokio::net::TcpListener;

        let provider = Arc::new(FailingTlsCredentialProvider);
        let (mut source, module, config) = test_state_with_tls_provider(provider)?;
        let (acceptor, _connector, _key_der, _key_pem) = test_tls_identity()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        let listener_id = source
            .resources
            .tls_listeners
            .add(TlsListener { listener, acceptor });
        source.reserve_network_handle()?;

        let mut replacement = source.new_state_for_reload(module, config)?;
        let report = source.transfer_runtime_resources_to(&mut replacement)?;

        assert_eq!(report.tls_listeners, 1);
        assert!(source.resources.tls_listeners.is_empty());
        let transferred = replacement
            .resources
            .tls_listeners
            .get(listener_id)
            .expect("live listener ID is preserved");
        assert_eq!(transferred.listener.local_addr()?, local_addr);
        drop(
            replacement
                .resources
                .tls_listeners
                .remove(listener_id)
                .expect("transferred listener remains removable"),
        );
        replacement.release_network_handle()?;
        Ok(())
    }

    #[tokio::test]
    async fn serialized_tls_listener_reinjects_without_private_key_bytes() -> anyhow::Result<()> {
        use lunatic_networking_api::TlsListener;
        use lunatic_process::state::ProcessState;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
            time::timeout,
        };
        use tokio_rustls::rustls::pki_types::ServerName;

        let provider = Arc::new(EphemeralTlsCredentialProvider::default());
        let (mut source, module, config) = test_state_with_tls_provider(provider)?;
        let (acceptor, connector, private_key_der, private_key_pem) = test_tls_identity()?;
        anyhow::ensure!(
            private_key_der.len() >= 40,
            "test private key is unexpectedly short"
        );
        let private_key_marker = &private_key_der[8..40];

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        let source_listener_id = source
            .resources
            .tls_listeners
            .add(TlsListener { listener, acceptor });
        source.reserve_network_handle()?;

        let snapshot = source
            .capture_resource_snapshot()?
            .expect("a TLS listener produces a resource snapshot");
        let bytes = snapshot.to_bytes()?;
        assert!(!bytes
            .windows(private_key_marker.len())
            .any(|window| window == private_key_marker));
        assert!(!bytes
            .windows(private_key_pem.len())
            .any(|window| window == private_key_pem));
        let debug = format!("{snapshot:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(&local_addr.to_string()));

        let decoded = ResourceMigrationSnapshot::from_bytes(&bytes)?;
        let replay = decoded.clone();
        drop(
            source
                .resources
                .tls_listeners
                .remove(source_listener_id)
                .expect("source listener remains present until snapshot capture completes"),
        );
        source.release_network_handle()?;

        let mut replacement = source.new_state_for_reload(module.clone(), config.clone())?;
        replacement.restore_resource_snapshot(decoded)?;
        assert_eq!(replacement.resources.tls_listeners.len(), 1);
        assert_eq!(replacement.network_resource_counts(), (1, 1));

        let mut replay_target = source.new_state_for_reload(module, config)?;
        let replay_error = replay_target
            .restore_resource_snapshot(replay)
            .expect_err("credential handles are single-use");
        assert_eq!(
            replay_error.to_string(),
            "TLS listener credential is unavailable"
        );
        assert!(replay_target.resources.tls_listeners.is_empty());
        assert_eq!(replay_target.network_resource_counts(), (0, 0));

        let restored_listener_id = *replacement
            .resources
            .tls_listeners
            .iter()
            .next()
            .expect("restored listener exists")
            .0;
        let restored = replacement
            .resources
            .tls_listeners
            .remove(restored_listener_id)
            .expect("restored listener remains removable");
        replacement.release_network_handle()?;

        let server_task = tokio::spawn(async move {
            let (tcp_stream, _) = restored.listener.accept().await?;
            let mut tls_stream = restored.acceptor.accept(tcp_stream).await?;
            let mut request = [0_u8; 4];
            tls_stream.read_exact(&mut request).await?;
            anyhow::ensure!(&request == b"ping", "unexpected TLS test payload");
            tls_stream.write_all(b"pong").await?;
            tls_stream.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        });

        let tcp_stream = TcpStream::connect(local_addr).await?;
        let domain = ServerName::try_from("localhost".to_string())?;
        let mut tls_stream = connector.connect(domain, tcp_stream).await?;
        tls_stream.write_all(b"ping").await?;
        let mut response = [0_u8; 4];
        tls_stream.read_exact(&mut response).await?;
        assert_eq!(&response, b"pong");
        let server_result = timeout(Duration::from_secs(5), server_task).await??;
        server_result?;
        Ok(())
    }

    #[tokio::test]
    async fn missing_tls_credential_fails_before_any_resource_mutation() -> anyhow::Result<()> {
        use lunatic_process::state::ProcessState;

        let provider = Arc::new(EphemeralTlsCredentialProvider::default());
        let (mut state, _module, _config) = test_state_with_tls_provider(provider)?;
        let mut snapshot = ResourceMigrationSnapshot::new();
        snapshot.add_tcp_listener(
            1,
            ResourceSnapshot::TcpListener {
                local_addr: "127.0.0.1:0".into(),
            },
        );
        snapshot.add_udp_socket(
            2,
            ResourceSnapshot::UdpSocket {
                local_addr: "127.0.0.1:0".into(),
            },
        );
        snapshot.add_tls_listener(
            3,
            ResourceSnapshot::TlsListener {
                local_addr: "127.0.0.1:0".into(),
                credential_handle: TlsCredentialHandle::from_bytes([0x5a; 16]),
            },
        );

        let error = state
            .restore_resource_snapshot(snapshot)
            .expect_err("an unknown credential must fail closed");
        assert_eq!(error.to_string(), "TLS listener credential is unavailable");
        assert!(state.resources.tcp_listeners.is_empty());
        assert!(state.resources.udp_sockets.is_empty());
        assert!(state.resources.tls_listeners.is_empty());
        assert_eq!(state.network_resource_counts(), (0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn later_tls_preflight_failure_rolls_back_prepared_listener_and_lease(
    ) -> anyhow::Result<()> {
        use lunatic_process::state::ProcessState;

        let (acceptor, _connector, _key_der, _key_pem) = test_tls_identity()?;
        let provider = Arc::new(FailAfterOneTlsCredentialProvider {
            material: Mutex::new(Some(TlsCredentialMaterial::new(acceptor))),
        });
        let (mut state, _module, _config) = test_state_with_tls_provider(provider)?;
        let mut snapshot = ResourceMigrationSnapshot::new();
        for (id, marker) in [(1, 0x11), (2, 0x22)] {
            snapshot.add_tls_listener(
                id,
                ResourceSnapshot::TlsListener {
                    local_addr: "127.0.0.1:0".into(),
                    credential_handle: TlsCredentialHandle::from_bytes([marker; 16]),
                },
            );
        }

        let error = state
            .restore_resource_snapshot(snapshot)
            .expect_err("the second provider failure must abort the whole TLS listener set");
        assert_eq!(error.to_string(), "TLS credential provider failed");
        assert!(state.resources.tls_listeners.is_empty());
        assert_eq!(state.network_resource_counts(), (0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn expired_tls_credential_fails_closed_with_stable_error() -> anyhow::Result<()> {
        use lunatic_process::state::ProcessState;

        let provider = Arc::new(EphemeralTlsCredentialProvider::new(Duration::ZERO));
        let (mut state, _module, _config) = test_state_with_tls_provider(provider.clone())?;
        let (acceptor, _connector, _key_der, _key_pem) = test_tls_identity()?;
        let handle = provider.provision(
            state.tls_credential_scope(),
            TlsCredentialMaterial::new(acceptor),
        )?;
        let mut snapshot = ResourceMigrationSnapshot::new();
        snapshot.add_tls_listener(
            1,
            ResourceSnapshot::TlsListener {
                local_addr: "127.0.0.1:0".into(),
                credential_handle: handle,
            },
        );

        let error = state
            .restore_resource_snapshot(snapshot)
            .expect_err("an expired credential must fail closed");
        assert_eq!(error.to_string(), "TLS listener credential has expired");
        assert!(state.resources.tls_listeners.is_empty());
        assert_eq!(state.network_resource_counts(), (0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn tls_provider_failure_is_stable_and_secret_free() -> anyhow::Result<()> {
        use lunatic_process::state::ProcessState;

        let provider = Arc::new(FailingTlsCredentialProvider);
        let (mut state, _module, _config) = test_state_with_tls_provider(provider)?;
        let handle = TlsCredentialHandle::from_bytes([0xa5; 16]);
        let mut snapshot = ResourceMigrationSnapshot::new();
        snapshot.add_tcp_listener(
            1,
            ResourceSnapshot::TcpListener {
                local_addr: "127.0.0.1:0".into(),
            },
        );
        snapshot.add_tls_listener(
            2,
            ResourceSnapshot::TlsListener {
                local_addr: "127.0.0.1:0".into(),
                credential_handle: handle,
            },
        );

        let error = state
            .restore_resource_snapshot(snapshot)
            .expect_err("provider failures must fail closed");
        let message = error.to_string();
        assert_eq!(message, "TLS credential provider failed");
        assert!(!message.contains(&format!("{:?}", handle.as_bytes())));
        assert!(!message.contains("127.0.0.1"));
        assert!(state.resources.tcp_listeners.is_empty());
        assert!(state.resources.tls_listeners.is_empty());
        assert_eq!(state.network_resource_counts(), (0, 0));
        Ok(())
    }

    #[test]
    fn serialized_resource_restore_without_runtime_fails_closed() -> anyhow::Result<()> {
        use lunatic_process::state::ProcessState;

        let provider = Arc::new(EphemeralTlsCredentialProvider::default());
        let runtime = tokio::runtime::Runtime::new()?;
        let (mut state, _module, _config) =
            runtime.block_on(async { test_state_with_tls_provider(provider) })?;
        drop(runtime);
        let mut snapshot = ResourceMigrationSnapshot::new();
        snapshot.add_tcp_listener(
            1,
            ResourceSnapshot::TcpListener {
                local_addr: "127.0.0.1:0".into(),
            },
        );

        let error = state
            .restore_resource_snapshot(snapshot)
            .expect_err("restore requires an active Tokio runtime");
        assert_eq!(
            error.to_string(),
            "resource restoration requires an active Tokio runtime"
        );
        assert!(state.resources.tcp_listeners.is_empty());
        assert_eq!(state.network_resource_counts(), (0, 0));
        Ok(())
    }

    #[test]
    fn listener_binding_without_tokio_io_driver_returns_errors() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let _guard = runtime.enter();
        let address = "127.0.0.1:0".parse()?;

        let tcp_result = std::panic::catch_unwind(|| super::bind_tcp_listener(address));
        let tcp_error = tcp_result
            .expect("TCP conversion panic must stay inside the binding boundary")
            .expect_err("an I/O-disabled runtime cannot register a TCP listener");
        assert_eq!(tcp_error.to_string(), "Tokio I/O driver is unavailable");

        let udp_result = std::panic::catch_unwind(|| super::bind_udp_socket(address));
        let udp_error = udp_result
            .expect("UDP conversion panic must stay inside the binding boundary")
            .expect_err("an I/O-disabled runtime cannot register a UDP socket");
        assert_eq!(udp_error.to_string(), "Tokio I/O driver is unavailable");
        Ok(())
    }

    #[tokio::test]
    async fn hot_reload_transfers_live_tls_stream_with_id_and_timeouts() -> anyhow::Result<()> {
        use lunatic_distributed::{control::cert, distributed::server::gen_node_cert};
        use lunatic_networking_api::{
            NetworkHandleLease, NetworkingCtx, TlsClientConnectionMetadata, TlsConnection,
        };
        use lunatic_process::{
            env::LunaticEnvironment,
            message::{DataMessage, Message, MessageNetworkResource},
            runtimes::wasmtime::WasmtimeRuntime,
            state::ProcessState,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
            sync::RwLock,
        };
        use tokio_rustls::{
            rustls::{
                pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName},
                ClientConfig, RootCertStore, ServerConfig,
            },
            TlsAcceptor, TlsConnector, TlsStream,
        };

        let root = cert::test_root_cert()?;
        let server_cert = gen_node_cert("localhost")?;
        let server_cert_pem = server_cert.serialize_pem_with_signer(&root)?;
        let server_key_pem = server_cert.serialize_private_key_pem();
        let server_cert_der = CertificateDer::from_pem_slice(server_cert_pem.as_bytes())?;
        let server_key_der = PrivateKeyDer::from_pem_slice(server_key_pem.as_bytes())?;

        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![server_cert_der], server_key_der)?;
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let server_addr = listener.local_addr()?;
        let server_task = tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await?;
            let mut tls_stream = acceptor.accept(tcp_stream).await?;
            let mut request = [0_u8; 4];
            tls_stream.read_exact(&mut request).await?;
            anyhow::ensure!(&request == b"ping", "unexpected TLS test payload");
            tls_stream.write_all(b"pong").await?;
            tls_stream.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        });

        let mut root_store = RootCertStore::empty();
        root_store.add(CertificateDer::from_pem_slice(
            root.certificate_pem().as_bytes(),
        )?)?;
        let client_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(client_config));
        let tcp_stream = TcpStream::connect(server_addr).await?;
        let peer_addr = tcp_stream.peer_addr().ok();
        let local_addr = tcp_stream.local_addr().ok();
        let domain = ServerName::try_from("localhost".to_string())?;
        let tls_stream = connector.connect(domain, tcp_stream).await?;
        let connection = Arc::new(TlsConnection::with_client_metadata(
            TlsStream::Client(tls_stream),
            TlsClientConnectionMetadata {
                server_name: "localhost".into(),
                port: server_addr.port(),
                peer_addr,
                local_addr,
                custom_root_certs: Vec::new(),
            },
        ));
        *connection.read_timeout.lock().await = Some(Duration::from_secs(5));
        *connection.write_timeout.lock().await = Some(Duration::from_secs(3));

        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.consume_fuel(true);
        let runtime = WasmtimeRuntime::new(&wasmtime_config)?;
        let raw_module = wat::parse_str(r#"(module (memory (export "memory") 1))"#)?;
        let module = Arc::new(runtime.compile_module(raw_module.into())?);
        let environment = Arc::new(LunaticEnvironment::new(0));
        let mut config = crate::DefaultProcessConfig::default();
        config.set_max_file_descriptors(2);
        config.set_max_network_connections(2);
        let config = Arc::new(config);
        let registry = Arc::new(RwLock::new(HashMap::new()));
        let mut old_state = super::DefaultProcessState::new(
            environment,
            None,
            runtime,
            module.clone(),
            config.clone(),
            registry,
        )?;
        let stream_id = old_state.resources.tls_streams.add(connection.clone());
        old_state.reserve_network_handle()?;
        old_state.reserve_network_handle()?;
        let source_quota = old_state
            .network_handle_quota()
            .expect("runtime state exposes a stable quota owner");
        let mut scratch = DataMessage::new(None, 0);
        scratch.add_network_resource(MessageNetworkResource::new(
            connection.clone(),
            NetworkHandleLease::from_existing(source_quota.clone()),
        ));
        old_state.message = Some(Message::Data(scratch));
        let mut replacement_state = old_state.new_state(module, config)?;

        let report = old_state.transfer_runtime_resources_to(&mut replacement_state)?;

        assert_eq!(report.tls_streams, 1);
        assert_eq!(report.total(), 1);
        assert!(old_state.resources.tls_streams.is_empty());
        assert_eq!(old_state.network_resource_counts(), (0, 0));
        assert_eq!(replacement_state.network_resource_counts(), (2, 2));
        let replacement_quota = replacement_state
            .network_handle_quota()
            .expect("replacement state exposes the transferred quota owner");
        assert!(Arc::ptr_eq(&source_quota, &replacement_quota));

        let Message::Data(mut scratch) = replacement_state
            .message
            .take()
            .expect("scratch message is transferred atomically")
        else {
            panic!("transferred the wrong scratch message kind")
        };
        let mut leased_stream = scratch
            .take_leased_tls_stream(0)
            .expect("scratch TLS stream keeps its quota lease");
        leased_stream.transfer_to(replacement_quota)?;
        let scratch_stream = leased_stream.into_table_resource();
        assert!(Arc::ptr_eq(&connection, &scratch_stream));
        replacement_state.resources.tls_streams.add(scratch_stream);
        assert_eq!(replacement_state.network_resource_counts(), (2, 2));

        let transferred = replacement_state
            .resources
            .tls_streams
            .get(stream_id)
            .expect("the guest TLS resource ID is preserved")
            .clone();
        assert!(Arc::ptr_eq(&connection, &transferred));
        assert_eq!(
            *transferred.read_timeout.lock().await,
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            *transferred.write_timeout.lock().await,
            Some(Duration::from_secs(3))
        );

        transferred.writer.lock().await.write_all(b"ping").await?;
        let mut response = [0_u8; 4];
        transferred
            .reader
            .lock()
            .await
            .read_exact(&mut response)
            .await?;
        assert_eq!(&response, b"pong");
        server_task.await??;

        Ok(())
    }

    #[tokio::test]
    async fn serialized_tls_stream_restore_fails_before_partial_restoration() -> anyhow::Result<()>
    {
        use lunatic_process::{
            env::LunaticEnvironment,
            resource_migration::{ResourceMigrationSnapshot, ResourceSnapshot},
            runtimes::wasmtime::WasmtimeRuntime,
            state::ProcessState,
        };
        use tokio::sync::RwLock;

        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.consume_fuel(true);
        let runtime = WasmtimeRuntime::new(&wasmtime_config)?;
        let raw_module = wat::parse_str(r#"(module (memory (export "memory") 1))"#)?;
        let module = Arc::new(runtime.compile_module(raw_module.into())?);
        let mut state = super::DefaultProcessState::new(
            Arc::new(LunaticEnvironment::new(0)),
            None,
            runtime,
            module,
            Arc::new(crate::DefaultProcessConfig::default()),
            Arc::new(RwLock::new(HashMap::new())),
        )?;

        let mut snapshot = ResourceMigrationSnapshot::new();
        snapshot.add_tcp_listener(
            11,
            ResourceSnapshot::TcpListener {
                local_addr: "127.0.0.1:0".into(),
            },
        );
        snapshot.add_tls_stream(
            12,
            ResourceSnapshot::TlsClientConnectionMetadata {
                server_name: "api.example.com".into(),
                port: 443,
                peer_addr: None,
                local_addr: None,
                custom_root_certs: Vec::new(),
                read_timeout_ms: None,
                write_timeout_ms: None,
            },
        );

        let error = state
            .restore_resource_snapshot(snapshot)
            .expect_err("serialized TLS stream restoration must be rejected");

        assert!(error
            .to_string()
            .contains("serialized TLS client stream restoration is unsupported"));
        assert!(state.resources.tcp_listeners.is_empty());
        assert!(state.resources.tls_streams.is_empty());

        let mut untrusted_snapshot = ResourceMigrationSnapshot::new();
        untrusted_snapshot.add_tls_stream(
            13,
            ResourceSnapshot::NonMigratable {
                resource_type: "private-key-type-marker".into(),
                reason: "private-key-reason-marker".into(),
            },
        );
        let error = state
            .restore_resource_snapshot(untrusted_snapshot)
            .expect_err("an unexpected TLS stream variant must be rejected");
        let message = error.to_string();
        assert_eq!(
            message,
            "serialized TLS stream restoration is unsupported \
             (resource 13, snapshot type redacted)"
        );
        assert!(!message.contains("private-key-type-marker"));
        assert!(!message.contains("private-key-reason-marker"));

        Ok(())
    }
}
