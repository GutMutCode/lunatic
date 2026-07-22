use std::{
    convert::TryFrom,
    fmt::Debug,
    fs,
    path::{Component, Path, PathBuf},
};

use lunatic_process::config::{
    ProcessConfig, DEFAULT_MAX_MAILBOX_MESSAGES, DEFAULT_MAX_MESSAGE_RESOURCES,
    DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_SIGNAL_QUEUE,
};
use lunatic_process_api::ProcessConfigCtx;
use lunatic_wasi_api::LunaticWasiConfigCtx;
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_MODULES: u32 = 64;
pub const DEFAULT_MAX_CONFIGS: u32 = 64;
pub const DEFAULT_MAX_MODULE_BYTES: u64 =
    lunatic_process::runtimes::wasmtime::DEFAULT_MAX_MODULE_BYTES as u64;
pub const DEFAULT_MAX_CONFIG_ENTRIES: u32 = 256;
pub const DEFAULT_MAX_CONFIG_BYTES: u64 = 1024 * 1024;
pub const DEFAULT_MAX_SQLITE_CONNECTIONS: u32 = 64;
pub const DEFAULT_MAX_SQLITE_STATEMENTS: u32 = 256;

#[derive(Clone, Serialize, Deserialize)]
pub struct DefaultProcessConfig {
    // Maximum amount of memory that can be used by processes in bytes
    max_memory: usize,
    // Maximum amount of compute expressed in units of 100k instructions.
    max_fuel: Option<u64>,
    // Can this process compile new WebAssembly modules
    can_compile_modules: bool,
    // Can this process create new configurations
    can_create_configs: bool,
    // Can this process spawn sub-processes
    can_spawn_processes: bool,
    // WASI configs
    preopened_dirs: Vec<(String, String)>,
    command_line_arguments: Vec<String>,
    environment_variables: Vec<(String, String)>,
    // Resource limits (Phase 3)
    max_table_elements: u32,
    max_file_descriptors: u32,
    max_network_connections: u32,
    #[serde(default = "default_max_mailbox_messages")]
    max_mailbox_messages: u32,
    #[serde(default = "default_max_signal_queue")]
    max_signal_queue: u32,
    #[serde(default = "default_max_message_size")]
    max_message_size: u64,
    #[serde(default = "default_max_message_resources")]
    max_message_resources: u32,
    #[serde(default = "default_max_modules")]
    max_modules: u32,
    #[serde(default = "default_max_configs")]
    max_configs: u32,
    #[serde(default = "default_max_module_bytes")]
    max_module_bytes: u64,
    #[serde(default = "default_max_config_entries")]
    max_config_entries: u32,
    #[serde(default = "default_max_config_bytes")]
    max_config_bytes: u64,
    #[serde(default = "default_max_sqlite_connections")]
    max_sqlite_connections: u32,
    #[serde(default = "default_max_sqlite_statements")]
    max_sqlite_statements: u32,
    // Can this process consume scoped host TLS credential handles. Keep new
    // serde-defaulted fields at the end for legacy MessagePack sequences.
    #[serde(default)]
    can_use_tls_credential_handles: bool,
}

const fn default_max_mailbox_messages() -> u32 {
    DEFAULT_MAX_MAILBOX_MESSAGES
}

const fn default_max_signal_queue() -> u32 {
    DEFAULT_MAX_SIGNAL_QUEUE
}

const fn default_max_message_size() -> u64 {
    DEFAULT_MAX_MESSAGE_SIZE
}

const fn default_max_message_resources() -> u32 {
    DEFAULT_MAX_MESSAGE_RESOURCES
}

const fn default_max_modules() -> u32 {
    DEFAULT_MAX_MODULES
}

const fn default_max_configs() -> u32 {
    DEFAULT_MAX_CONFIGS
}

const fn default_max_module_bytes() -> u64 {
    DEFAULT_MAX_MODULE_BYTES
}

const fn default_max_config_entries() -> u32 {
    DEFAULT_MAX_CONFIG_ENTRIES
}

const fn default_max_config_bytes() -> u64 {
    DEFAULT_MAX_CONFIG_BYTES
}

const fn default_max_sqlite_connections() -> u32 {
    DEFAULT_MAX_SQLITE_CONNECTIONS
}

const fn default_max_sqlite_statements() -> u32 {
    DEFAULT_MAX_SQLITE_STATEMENTS
}

impl Default for DefaultProcessConfig {
    fn default() -> Self {
        Self {
            max_memory: 10_000_000, // 10MB default
            max_fuel: None,
            can_compile_modules: false,
            can_create_configs: false,
            can_spawn_processes: false,
            can_use_tls_credential_handles: false,
            preopened_dirs: Vec::new(),
            command_line_arguments: Vec::new(),
            environment_variables: Vec::new(),
            max_table_elements: 100_000,
            max_file_descriptors: 1024,
            max_network_connections: 1024,
            max_mailbox_messages: DEFAULT_MAX_MAILBOX_MESSAGES,
            max_signal_queue: DEFAULT_MAX_SIGNAL_QUEUE,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_message_resources: DEFAULT_MAX_MESSAGE_RESOURCES,
            max_modules: DEFAULT_MAX_MODULES,
            max_configs: DEFAULT_MAX_CONFIGS,
            max_module_bytes: DEFAULT_MAX_MODULE_BYTES,
            max_config_entries: DEFAULT_MAX_CONFIG_ENTRIES,
            max_config_bytes: DEFAULT_MAX_CONFIG_BYTES,
            max_sqlite_connections: DEFAULT_MAX_SQLITE_CONNECTIONS,
            max_sqlite_statements: DEFAULT_MAX_SQLITE_STATEMENTS,
        }
    }
}

impl Debug for DefaultProcessConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("DefaultProcessConfig")
            .field("max_memory", &self.max_memory)
            .field("max_fuel", &self.max_fuel)
            .field("can_compile_modules", &self.can_compile_modules)
            .field("can_create_configs", &self.can_create_configs)
            .field("can_spawn_processes", &self.can_spawn_processes)
            .field(
                "can_use_tls_credential_handles",
                &self.can_use_tls_credential_handles,
            )
            .field("preopened_dir_count", &self.preopened_dirs.len())
            .field("argument_count", &self.command_line_arguments.len())
            .field(
                "environment_variable_count",
                &self.environment_variables.len(),
            )
            .field("max_table_elements", &self.max_table_elements)
            .field("max_file_descriptors", &self.max_file_descriptors)
            .field("max_network_connections", &self.max_network_connections)
            .field("max_mailbox_messages", &self.max_mailbox_messages)
            .field("max_signal_queue", &self.max_signal_queue)
            .field("max_message_size", &self.max_message_size)
            .field("max_message_resources", &self.max_message_resources)
            .field("max_modules", &self.max_modules)
            .field("max_configs", &self.max_configs)
            .field("max_module_bytes", &self.max_module_bytes)
            .field("max_config_entries", &self.max_config_entries)
            .field("max_config_bytes", &self.max_config_bytes)
            .field("max_sqlite_connections", &self.max_sqlite_connections)
            .field("max_sqlite_statements", &self.max_sqlite_statements)
            .finish()
    }
}

impl ProcessConfig for DefaultProcessConfig {
    fn set_max_fuel(&mut self, max_fuel: Option<u64>) {
        self.max_fuel = max_fuel;
    }

    fn get_max_fuel(&self) -> Option<u64> {
        self.max_fuel
    }

    fn set_max_memory(&mut self, max_memory: usize) {
        self.max_memory = max_memory
    }

    fn get_max_memory(&self) -> usize {
        self.max_memory
    }

    fn set_max_mailbox_messages(&mut self, max: u32) {
        self.max_mailbox_messages = max;
    }

    fn get_max_mailbox_messages(&self) -> u32 {
        self.max_mailbox_messages
    }

    fn set_max_signal_queue(&mut self, max: u32) {
        self.max_signal_queue = max;
    }

    fn get_max_signal_queue(&self) -> u32 {
        self.max_signal_queue
    }

    fn set_max_message_size(&mut self, max: u64) {
        self.max_message_size = max;
    }

    fn get_max_message_size(&self) -> u64 {
        self.max_message_size
    }

    fn set_max_message_resources(&mut self, max: u32) {
        self.max_message_resources = max;
    }

    fn get_max_message_resources(&self) -> u32 {
        self.max_message_resources
    }

    fn new_child_config(&self) -> Result<Self, String> {
        Ok(Self {
            max_memory: self.max_memory,
            max_fuel: self.max_fuel,
            can_compile_modules: false,
            can_create_configs: false,
            can_spawn_processes: false,
            can_use_tls_credential_handles: false,
            preopened_dirs: Vec::new(),
            command_line_arguments: Vec::new(),
            environment_variables: Vec::new(),
            max_table_elements: self.max_table_elements,
            max_file_descriptors: self.max_file_descriptors,
            max_network_connections: self.max_network_connections,
            max_mailbox_messages: self.max_mailbox_messages,
            max_signal_queue: self.max_signal_queue,
            max_message_size: self.max_message_size,
            max_message_resources: self.max_message_resources,
            max_modules: self.max_modules,
            max_configs: self.max_configs,
            max_module_bytes: self.max_module_bytes,
            max_config_entries: self.max_config_entries,
            max_config_bytes: self.max_config_bytes,
            max_sqlite_connections: self.max_sqlite_connections,
            max_sqlite_statements: self.max_sqlite_statements,
        })
    }

    fn validate_child_config(&self, child: &Self) -> Result<(), String> {
        child.validate_runtime_limits()?;
        if child.can_compile_modules && !self.can_compile_modules {
            return Err("compile-module capability exceeds parent authority".into());
        }
        if child.can_create_configs && !self.can_create_configs {
            return Err("create-config capability exceeds parent authority".into());
        }
        if child.can_spawn_processes && !self.can_spawn_processes {
            return Err("spawn capability exceeds parent authority".into());
        }
        if child.can_use_tls_credential_handles && !self.can_use_tls_credential_handles {
            return Err("TLS credential-handle capability exceeds parent authority".into());
        }
        if child.max_memory > self.max_memory {
            return Err(format!(
                "max_memory {} exceeds parent ceiling {}",
                child.max_memory, self.max_memory
            ));
        }
        match (self.max_fuel, child.max_fuel) {
            (Some(parent), Some(child)) if child <= parent => {}
            (Some(parent), Some(child)) => {
                return Err(format!("max_fuel {child} exceeds parent ceiling {parent}"));
            }
            (Some(parent), None) => {
                return Err(format!(
                    "unlimited fuel exceeds finite parent ceiling {parent}"
                ));
            }
            (None, _) => {}
        }
        if child.max_table_elements > self.max_table_elements {
            return Err(format!(
                "max_table_elements {} exceeds parent ceiling {}",
                child.max_table_elements, self.max_table_elements
            ));
        }
        if child.max_file_descriptors > self.max_file_descriptors {
            return Err(format!(
                "max_file_descriptors {} exceeds parent ceiling {}",
                child.max_file_descriptors, self.max_file_descriptors
            ));
        }
        if child.max_network_connections > self.max_network_connections {
            return Err(format!(
                "max_network_connections {} exceeds parent ceiling {}",
                child.max_network_connections, self.max_network_connections
            ));
        }
        if child.max_mailbox_messages > self.max_mailbox_messages {
            return Err(format!(
                "max_mailbox_messages {} exceeds parent ceiling {}",
                child.max_mailbox_messages, self.max_mailbox_messages
            ));
        }
        if child.max_signal_queue > self.max_signal_queue {
            return Err(format!(
                "max_signal_queue {} exceeds parent ceiling {}",
                child.max_signal_queue, self.max_signal_queue
            ));
        }
        if child.max_message_size > self.max_message_size {
            return Err(format!(
                "max_message_size {} exceeds parent ceiling {}",
                child.max_message_size, self.max_message_size
            ));
        }
        if child.max_message_resources > self.max_message_resources {
            return Err(format!(
                "max_message_resources {} exceeds parent ceiling {}",
                child.max_message_resources, self.max_message_resources
            ));
        }
        if child.max_modules > self.max_modules {
            return Err(format!(
                "max_modules {} exceeds parent ceiling {}",
                child.max_modules, self.max_modules
            ));
        }
        if child.max_configs > self.max_configs {
            return Err(format!(
                "max_configs {} exceeds parent ceiling {}",
                child.max_configs, self.max_configs
            ));
        }
        if child.max_module_bytes > self.max_module_bytes {
            return Err(format!(
                "max_module_bytes {} exceeds parent ceiling {}",
                child.max_module_bytes, self.max_module_bytes
            ));
        }
        if child.max_config_entries > self.max_config_entries {
            return Err(format!(
                "max_config_entries {} exceeds parent ceiling {}",
                child.max_config_entries, self.max_config_entries
            ));
        }
        if child.max_config_bytes > self.max_config_bytes {
            return Err(format!(
                "max_config_bytes {} exceeds parent ceiling {}",
                child.max_config_bytes, self.max_config_bytes
            ));
        }
        if child.max_sqlite_connections > self.max_sqlite_connections {
            return Err(format!(
                "max_sqlite_connections {} exceeds parent ceiling {}",
                child.max_sqlite_connections, self.max_sqlite_connections
            ));
        }
        if child.max_sqlite_statements > self.max_sqlite_statements {
            return Err(format!(
                "max_sqlite_statements {} exceeds parent ceiling {}",
                child.max_sqlite_statements, self.max_sqlite_statements
            ));
        }
        for (_, dir) in &child.preopened_dirs {
            self.can_delegate_preopen_dir(Path::new(dir))?;
        }
        Ok(())
    }

    fn validate_distributed_config(&self) -> Result<(), String> {
        self.validate_runtime_limits()?;
        let receiver_ceiling = Self::default();
        if self.can_compile_modules && !receiver_ceiling.can_compile_modules {
            return Err("compile-module capability exceeds receiver authority".into());
        }
        if self.can_create_configs && !receiver_ceiling.can_create_configs {
            return Err("create-config capability exceeds receiver authority".into());
        }
        if self.can_spawn_processes && !receiver_ceiling.can_spawn_processes {
            return Err("spawn capability exceeds receiver authority".into());
        }
        if self.can_use_tls_credential_handles && !receiver_ceiling.can_use_tls_credential_handles {
            return Err("TLS credential-handle capability exceeds receiver authority".into());
        }
        if self.max_memory > receiver_ceiling.max_memory {
            return Err(format!(
                "max_memory {} exceeds receiver ceiling {}",
                self.max_memory, receiver_ceiling.max_memory
            ));
        }
        if self.max_table_elements > receiver_ceiling.max_table_elements {
            return Err(format!(
                "max_table_elements {} exceeds receiver ceiling {}",
                self.max_table_elements, receiver_ceiling.max_table_elements
            ));
        }
        if self.max_file_descriptors > receiver_ceiling.max_file_descriptors {
            return Err(format!(
                "max_file_descriptors {} exceeds receiver ceiling {}",
                self.max_file_descriptors, receiver_ceiling.max_file_descriptors
            ));
        }
        if self.max_network_connections > receiver_ceiling.max_network_connections {
            return Err(format!(
                "max_network_connections {} exceeds receiver ceiling {}",
                self.max_network_connections, receiver_ceiling.max_network_connections
            ));
        }
        if self.max_mailbox_messages > receiver_ceiling.max_mailbox_messages {
            return Err(format!(
                "max_mailbox_messages {} exceeds receiver ceiling {}",
                self.max_mailbox_messages, receiver_ceiling.max_mailbox_messages
            ));
        }
        if self.max_signal_queue > receiver_ceiling.max_signal_queue {
            return Err(format!(
                "max_signal_queue {} exceeds receiver ceiling {}",
                self.max_signal_queue, receiver_ceiling.max_signal_queue
            ));
        }
        if self.max_message_size > receiver_ceiling.max_message_size {
            return Err(format!(
                "max_message_size {} exceeds receiver ceiling {}",
                self.max_message_size, receiver_ceiling.max_message_size
            ));
        }
        if self.max_message_resources > receiver_ceiling.max_message_resources {
            return Err(format!(
                "max_message_resources {} exceeds receiver ceiling {}",
                self.max_message_resources, receiver_ceiling.max_message_resources
            ));
        }
        if self.max_modules > receiver_ceiling.max_modules {
            return Err(format!(
                "max_modules {} exceeds receiver ceiling {}",
                self.max_modules, receiver_ceiling.max_modules
            ));
        }
        if self.max_configs > receiver_ceiling.max_configs {
            return Err(format!(
                "max_configs {} exceeds receiver ceiling {}",
                self.max_configs, receiver_ceiling.max_configs
            ));
        }
        if self.max_module_bytes > receiver_ceiling.max_module_bytes {
            return Err(format!(
                "max_module_bytes {} exceeds receiver ceiling {}",
                self.max_module_bytes, receiver_ceiling.max_module_bytes
            ));
        }
        if self.max_config_entries > receiver_ceiling.max_config_entries {
            return Err(format!(
                "max_config_entries {} exceeds receiver ceiling {}",
                self.max_config_entries, receiver_ceiling.max_config_entries
            ));
        }
        if self.max_config_bytes > receiver_ceiling.max_config_bytes {
            return Err(format!(
                "max_config_bytes {} exceeds receiver ceiling {}",
                self.max_config_bytes, receiver_ceiling.max_config_bytes
            ));
        }
        if self.max_sqlite_connections > receiver_ceiling.max_sqlite_connections {
            return Err(format!(
                "max_sqlite_connections {} exceeds receiver ceiling {}",
                self.max_sqlite_connections, receiver_ceiling.max_sqlite_connections
            ));
        }
        if self.max_sqlite_statements > receiver_ceiling.max_sqlite_statements {
            return Err(format!(
                "max_sqlite_statements {} exceeds receiver ceiling {}",
                self.max_sqlite_statements, receiver_ceiling.max_sqlite_statements
            ));
        }
        if !self.preopened_dirs.is_empty() {
            return Err(
                "filesystem preopens are host-local and cannot be delegated to a remote node without an explicit receiver policy"
                    .into(),
            );
        }
        Ok(())
    }
}

impl LunaticWasiConfigCtx for DefaultProcessConfig {
    fn add_environment_variable(&mut self, key: String, value: String) {
        self.environment_variables.push((key, value));
    }

    fn add_command_line_argument(&mut self, argument: String) {
        self.command_line_arguments.push(argument);
    }

    fn preopen_dir(&mut self, dir: String) {
        self.add_preopened_dir(dir);
    }
}

impl DefaultProcessConfig {
    pub(crate) fn validate_runtime_limits(&self) -> Result<(), String> {
        if self.max_mailbox_messages == 0 {
            return Err("max_mailbox_messages must be greater than zero".into());
        }
        if self.max_signal_queue == 0 {
            return Err("max_signal_queue must be greater than zero".into());
        }
        if self.max_message_size == 0 {
            return Err("max_message_size must be greater than zero".into());
        }
        self.validate_retained_config_limits()?;
        Ok(())
    }

    fn validate_retained_config_limits(&self) -> Result<(), String> {
        self.validate_retained_values_against(self)
    }

    pub(crate) fn validate_retained_values_against(&self, ceiling: &Self) -> Result<(), String> {
        let entries = self
            .command_line_arguments
            .len()
            .checked_add(self.environment_variables.len())
            .and_then(|entries| entries.checked_add(self.preopened_dirs.len()))
            .ok_or_else(|| "retained config entry accounting overflow".to_owned())?;
        let entries = u64::try_from(entries)
            .map_err(|_| "retained config entry accounting overflow".to_owned())?;
        if entries > u64::from(ceiling.max_config_entries) {
            return Err(format!(
                "retained config entry count {entries} exceeds max_config_entries {}",
                ceiling.max_config_entries
            ));
        }

        let mut bytes = 0_u64;
        for argument in &self.command_line_arguments {
            bytes = checked_add_retained_bytes(bytes, argument.len())?;
        }
        for (key, value) in &self.environment_variables {
            bytes = checked_add_retained_bytes(bytes, key.len())?;
            bytes = checked_add_retained_bytes(bytes, value.len())?;
        }
        for (guest_path, resolved_path) in &self.preopened_dirs {
            bytes = checked_add_retained_bytes(bytes, guest_path.len())?;
            bytes = checked_add_retained_bytes(bytes, resolved_path.len())?;
        }
        if bytes > ceiling.max_config_bytes {
            return Err(format!(
                "retained config byte count {bytes} exceeds max_config_bytes {}",
                ceiling.max_config_bytes
            ));
        }
        Ok(())
    }

    pub fn preopened_dirs(&self) -> &[(String, String)] {
        &self.preopened_dirs
    }

    pub fn get_max_table_elements(&self) -> u32 {
        self.max_table_elements
    }

    pub fn set_max_table_elements(&mut self, max: u32) {
        self.max_table_elements = max;
    }

    pub fn get_max_file_descriptors(&self) -> u32 {
        self.max_file_descriptors
    }

    pub fn set_max_file_descriptors(&mut self, max: u32) {
        self.max_file_descriptors = max;
    }

    pub fn get_max_network_connections(&self) -> u32 {
        self.max_network_connections
    }

    pub fn set_max_network_connections(&mut self, max: u32) {
        self.max_network_connections = max;
    }

    pub fn get_max_mailbox_messages(&self) -> u32 {
        self.max_mailbox_messages
    }

    pub fn set_max_mailbox_messages(&mut self, max: u32) {
        self.max_mailbox_messages = max;
    }

    pub fn get_max_signal_queue(&self) -> u32 {
        self.max_signal_queue
    }

    pub fn set_max_signal_queue(&mut self, max: u32) {
        self.max_signal_queue = max;
    }

    pub fn get_max_message_size(&self) -> u64 {
        self.max_message_size
    }

    pub fn set_max_message_size(&mut self, max: u64) {
        self.max_message_size = max;
    }

    pub fn get_max_message_resources(&self) -> u32 {
        self.max_message_resources
    }

    pub fn set_max_message_resources(&mut self, max: u32) {
        self.max_message_resources = max;
    }

    pub fn get_max_modules(&self) -> u32 {
        self.max_modules
    }

    pub fn set_max_modules(&mut self, max: u32) {
        self.max_modules = max;
    }

    pub fn get_max_configs(&self) -> u32 {
        self.max_configs
    }

    pub fn set_max_configs(&mut self, max: u32) {
        self.max_configs = max;
    }

    pub fn get_max_module_bytes(&self) -> u64 {
        self.max_module_bytes
    }

    pub fn set_max_module_bytes(&mut self, max: u64) {
        self.max_module_bytes = max;
    }

    pub fn get_max_config_entries(&self) -> u32 {
        self.max_config_entries
    }

    pub fn set_max_config_entries(&mut self, max: u32) {
        self.max_config_entries = max;
    }

    pub fn get_max_config_bytes(&self) -> u64 {
        self.max_config_bytes
    }

    pub fn set_max_config_bytes(&mut self, max: u64) {
        self.max_config_bytes = max;
    }

    pub fn get_max_sqlite_connections(&self) -> u32 {
        self.max_sqlite_connections
    }

    pub fn set_max_sqlite_connections(&mut self, max: u32) {
        self.max_sqlite_connections = max;
    }

    pub fn get_max_sqlite_statements(&self) -> u32 {
        self.max_sqlite_statements
    }

    pub fn set_max_sqlite_statements(&mut self, max: u32) {
        self.max_sqlite_statements = max;
    }

    /// Grant access to the given directory with this config.
    pub fn preopen_dir<S: Into<String>>(&mut self, dir: S) {
        self.add_preopened_dir(dir.into());
    }

    pub fn set_command_line_arguments(&mut self, args: Vec<String>) {
        self.command_line_arguments = args;
    }

    pub fn command_line_arguments(&self) -> &Vec<String> {
        &self.command_line_arguments
    }

    pub fn set_environment_variables(&mut self, envs: Vec<(String, String)>) {
        self.environment_variables = envs;
    }

    pub fn environment_variables(&self) -> &Vec<(String, String)> {
        &self.environment_variables
    }

    fn add_preopened_dir(&mut self, dir: String) {
        let path = if dir == "~" {
            dirs::home_dir().unwrap_or_else(|| PathBuf::from(&dir))
        } else {
            PathBuf::from(&dir)
        };
        let resolved_path = get_absolute_path(&path).unwrap_or(path);
        self.preopened_dirs
            .push((dir, resolved_path.to_string_lossy().into_owned()));
    }

    pub(crate) fn can_delegate_preopen_dir(&self, path: &Path) -> Result<(), String> {
        let requested = fs::canonicalize(path).map_err(|error| {
            format!(
                "preopen directory '{}' cannot be resolved: {error}",
                path.display()
            )
        })?;
        if !requested.is_dir() {
            return Err(format!(
                "preopen path '{}' is not a directory",
                path.display()
            ));
        }

        let allowed = self.preopened_dirs.iter().any(|(_, parent)| {
            fs::canonicalize(parent)
                .ok()
                .filter(|parent| parent.is_dir())
                .is_some_and(|parent| path_is_ancestor(&parent, &requested))
        });
        if allowed {
            Ok(())
        } else {
            Err(format!(
                "preopen directory '{}' is outside parent filesystem authority",
                path.display()
            ))
        }
    }
}

impl ProcessConfigCtx for DefaultProcessConfig {
    fn can_compile_modules(&self) -> bool {
        self.can_compile_modules
    }

    fn set_can_compile_modules(&mut self, can: bool) {
        self.can_compile_modules = can
    }

    fn can_create_configs(&self) -> bool {
        self.can_create_configs
    }

    fn set_can_create_configs(&mut self, can: bool) {
        self.can_create_configs = can
    }

    fn can_spawn_processes(&self) -> bool {
        self.can_spawn_processes
    }

    fn set_can_spawn_processes(&mut self, can: bool) {
        self.can_spawn_processes = can
    }

    fn can_use_tls_credential_handles(&self) -> bool {
        self.can_use_tls_credential_handles
    }

    fn set_can_use_tls_credential_handles(&mut self, can: bool) {
        self.can_use_tls_credential_handles = can
    }

    fn get_max_table_elements(&self) -> u32 {
        self.max_table_elements
    }

    fn set_max_table_elements(&mut self, max: u32) {
        self.max_table_elements = max;
    }

    fn get_max_file_descriptors(&self) -> u32 {
        self.max_file_descriptors
    }

    fn set_max_file_descriptors(&mut self, max: u32) {
        self.max_file_descriptors = max;
    }

    fn get_max_network_connections(&self) -> u32 {
        self.max_network_connections
    }

    fn set_max_network_connections(&mut self, max: u32) {
        self.max_network_connections = max;
    }

    fn get_max_mailbox_messages(&self) -> u32 {
        self.max_mailbox_messages
    }

    fn set_max_mailbox_messages(&mut self, max: u32) {
        self.max_mailbox_messages = max;
    }

    fn get_max_signal_queue(&self) -> u32 {
        self.max_signal_queue
    }

    fn set_max_signal_queue(&mut self, max: u32) {
        self.max_signal_queue = max;
    }

    fn get_max_message_size(&self) -> u64 {
        self.max_message_size
    }

    fn set_max_message_size(&mut self, max: u64) {
        self.max_message_size = max;
    }

    fn get_max_message_resources(&self) -> u32 {
        self.max_message_resources
    }

    fn set_max_message_resources(&mut self, max: u32) {
        self.max_message_resources = max;
    }

    fn get_max_modules(&self) -> u32 {
        self.max_modules
    }

    fn set_max_modules(&mut self, max: u32) {
        self.max_modules = max;
    }

    fn get_max_configs(&self) -> u32 {
        self.max_configs
    }

    fn set_max_configs(&mut self, max: u32) {
        self.max_configs = max;
    }

    fn get_max_module_bytes(&self) -> u64 {
        self.max_module_bytes
    }

    fn set_max_module_bytes(&mut self, max: u64) {
        self.max_module_bytes = max;
    }

    fn get_max_config_entries(&self) -> u32 {
        self.max_config_entries
    }

    fn set_max_config_entries(&mut self, max: u32) {
        self.max_config_entries = max;
    }

    fn get_max_config_bytes(&self) -> u64 {
        self.max_config_bytes
    }

    fn set_max_config_bytes(&mut self, max: u64) {
        self.max_config_bytes = max;
    }

    fn get_max_sqlite_connections(&self) -> u32 {
        self.max_sqlite_connections
    }

    fn set_max_sqlite_connections(&mut self, max: u32) {
        self.max_sqlite_connections = max;
    }

    fn get_max_sqlite_statements(&self) -> u32 {
        self.max_sqlite_statements
    }

    fn set_max_sqlite_statements(&mut self, max: u32) {
        self.max_sqlite_statements = max;
    }

    fn can_access_fs_location(&self, path: &std::path::Path) -> Result<(), String> {
        let (file_path, parent_dir) = match strip_file(path) {
            Ok(p) => p,
            Err(e) => {
                return Err(e.to_string());
            }
        };
        let has_access = self
            .preopened_dirs()
            .iter()
            .filter_map(|(_, dir)| get_absolute_path(Path::new(dir)).ok())
            .any(|dir| dir.exists() && path_is_ancestor(&dir, &parent_dir));

        match has_access {
            true => Ok(()),
            false => Err(format!("Permission to '{file_path:?}' denied")),
        }
    }
}

fn checked_add_retained_bytes(current: u64, bytes: usize) -> Result<u64, String> {
    let bytes =
        u64::try_from(bytes).map_err(|_| "retained config byte accounting overflow".to_owned())?;
    current
        .checked_add(bytes)
        .ok_or_else(|| "retained config byte accounting overflow".to_owned())
}

fn path_is_ancestor(ancestor: &Path, descendant: &Path) -> bool {
    if !ancestor.is_dir() {
        return false;
    }
    descendant.starts_with(ancestor)
}

// returns a tuple of paths, where the first is the full resolved canonicalized path
// and the second one is stripped of the file extension, pointing to the parent directory
// of the file that a program is trying to access
fn strip_file(path: &Path) -> std::io::Result<(PathBuf, PathBuf)> {
    let absolute_path = get_absolute_path(path)?;
    if absolute_path.is_file() {
        let parent = absolute_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| absolute_path.clone());
        return Ok((absolute_path, parent));
    }
    Ok((absolute_path.clone(), absolute_path))
}

fn get_absolute_path(path: &std::path::Path) -> std::io::Result<PathBuf> {
    let path = if path.is_relative() {
        Path::join(std::env::current_dir()?.as_path(), path)
    } else {
        path.to_path_buf()
    };
    canonicalize_with_missing(&normalize_path(&path))
}

/// Canonicalizes the nearest existing ancestor and then restores any missing
/// suffix. This keeps create-file checks useful while resolving symlinked
/// ancestors before the containment comparison.
fn canonicalize_with_missing(path: &Path) -> std::io::Result<PathBuf> {
    let mut existing = path.to_path_buf();
    let mut missing = Vec::new();
    while !existing.exists() {
        let component = existing.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No existing ancestor for '{}'", path.display()),
            )
        })?;
        missing.push(component.to_os_string());
        if !existing.pop() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No existing ancestor for '{}'", path.display()),
            ));
        }
    }

    let mut canonical = fs::canonicalize(existing)?;
    for component in missing.iter().rev() {
        canonical.push(component);
    }
    Ok(normalize_path(&canonical))
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::config::{get_absolute_path, path_is_ancestor};
    use lunatic_process::config::ProcessConfig;
    use lunatic_process_api::ProcessConfigCtx;
    use serde::Serialize;

    use super::{
        normalize_path, DefaultProcessConfig, DEFAULT_MAX_CONFIGS, DEFAULT_MAX_CONFIG_BYTES,
        DEFAULT_MAX_CONFIG_ENTRIES, DEFAULT_MAX_MODULES, DEFAULT_MAX_MODULE_BYTES,
        DEFAULT_MAX_SQLITE_CONNECTIONS, DEFAULT_MAX_SQLITE_STATEMENTS,
    };

    #[test]
    fn default_config_denies_privileged_capabilities() {
        let config = DefaultProcessConfig::default();

        assert!(!config.can_compile_modules());
        assert!(!config.can_create_configs());
        assert!(!config.can_spawn_processes());
        assert!(!config.can_use_tls_credential_handles());
        assert!(config.preopened_dirs().is_empty());
        assert_eq!(config.get_max_modules(), DEFAULT_MAX_MODULES);
        assert_eq!(config.get_max_configs(), DEFAULT_MAX_CONFIGS);
        assert_eq!(config.get_max_module_bytes(), DEFAULT_MAX_MODULE_BYTES);
        assert_eq!(config.get_max_config_entries(), DEFAULT_MAX_CONFIG_ENTRIES);
        assert_eq!(config.get_max_config_bytes(), DEFAULT_MAX_CONFIG_BYTES);
        assert_eq!(
            config.get_max_sqlite_connections(),
            DEFAULT_MAX_SQLITE_CONNECTIONS
        );
        assert_eq!(
            config.get_max_sqlite_statements(),
            DEFAULT_MAX_SQLITE_STATEMENTS
        );
    }

    #[test]
    fn new_resource_ceilings_use_serde_defaults_for_legacy_configs() {
        let mut serialized = serde_json::to_value(DefaultProcessConfig::default()).unwrap();
        let object = serialized.as_object_mut().unwrap();
        for field in [
            "max_modules",
            "max_configs",
            "max_module_bytes",
            "max_config_entries",
            "max_config_bytes",
            "max_sqlite_connections",
            "max_sqlite_statements",
            "can_use_tls_credential_handles",
        ] {
            object.remove(field);
        }

        let config: DefaultProcessConfig = serde_json::from_value(serialized).unwrap();
        assert_eq!(config.get_max_modules(), DEFAULT_MAX_MODULES);
        assert_eq!(config.get_max_configs(), DEFAULT_MAX_CONFIGS);
        assert_eq!(config.get_max_module_bytes(), DEFAULT_MAX_MODULE_BYTES);
        assert_eq!(config.get_max_config_entries(), DEFAULT_MAX_CONFIG_ENTRIES);
        assert_eq!(config.get_max_config_bytes(), DEFAULT_MAX_CONFIG_BYTES);
        assert_eq!(
            config.get_max_sqlite_connections(),
            DEFAULT_MAX_SQLITE_CONNECTIONS
        );
        assert_eq!(
            config.get_max_sqlite_statements(),
            DEFAULT_MAX_SQLITE_STATEMENTS
        );
        assert!(!config.can_use_tls_credential_handles());
    }

    #[test]
    fn tls_credential_capability_defaults_false_for_legacy_messagepack_configs() {
        #[derive(Serialize)]
        struct LegacyDefaultProcessConfig {
            max_memory: usize,
            max_fuel: Option<u64>,
            can_compile_modules: bool,
            can_create_configs: bool,
            can_spawn_processes: bool,
            preopened_dirs: Vec<(String, String)>,
            command_line_arguments: Vec<String>,
            environment_variables: Vec<(String, String)>,
            max_table_elements: u32,
            max_file_descriptors: u32,
            max_network_connections: u32,
            max_mailbox_messages: u32,
            max_signal_queue: u32,
            max_message_size: u64,
            max_message_resources: u32,
            max_modules: u32,
            max_configs: u32,
            max_module_bytes: u64,
            max_config_entries: u32,
            max_config_bytes: u64,
            max_sqlite_connections: u32,
            max_sqlite_statements: u32,
        }

        let config = DefaultProcessConfig::default();
        let legacy = LegacyDefaultProcessConfig {
            max_memory: config.max_memory,
            max_fuel: config.max_fuel,
            can_compile_modules: config.can_compile_modules,
            can_create_configs: config.can_create_configs,
            can_spawn_processes: config.can_spawn_processes,
            preopened_dirs: config.preopened_dirs.clone(),
            command_line_arguments: config.command_line_arguments.clone(),
            environment_variables: config.environment_variables.clone(),
            max_table_elements: config.max_table_elements,
            max_file_descriptors: config.max_file_descriptors,
            max_network_connections: config.max_network_connections,
            max_mailbox_messages: config.max_mailbox_messages,
            max_signal_queue: config.max_signal_queue,
            max_message_size: config.max_message_size,
            max_message_resources: config.max_message_resources,
            max_modules: config.max_modules,
            max_configs: config.max_configs,
            max_module_bytes: config.max_module_bytes,
            max_config_entries: config.max_config_entries,
            max_config_bytes: config.max_config_bytes,
            max_sqlite_connections: config.max_sqlite_connections,
            max_sqlite_statements: config.max_sqlite_statements,
        };

        let bytes = rmp_serde::to_vec(&legacy).unwrap();
        let decoded: DefaultProcessConfig = rmp_serde::from_slice(&bytes).unwrap();
        assert!(!decoded.can_use_tls_credential_handles());
    }

    #[test]
    fn retained_config_limits_count_entries_and_utf8_bytes() {
        let mut config = DefaultProcessConfig::default();
        config.set_max_config_entries(2);
        config.set_max_config_bytes(7);
        config.set_command_line_arguments(vec!["é".into()]);
        config.set_environment_variables(vec![("KEY".into(), "ok".into())]);
        config.validate_runtime_limits().unwrap();

        config.set_max_config_bytes(6);
        assert!(config
            .validate_runtime_limits()
            .unwrap_err()
            .contains("max_config_bytes"));

        config.set_max_config_bytes(7);
        config.set_command_line_arguments(vec!["é".into(), "extra".into()]);
        assert!(config
            .validate_runtime_limits()
            .unwrap_err()
            .contains("max_config_entries"));
    }

    #[test]
    fn child_and_distributed_validation_attenuate_new_ceilings() {
        let mut parent = DefaultProcessConfig::default();
        parent.set_max_modules(4);
        parent.set_max_configs(3);
        parent.set_max_module_bytes(2_048);
        parent.set_max_config_entries(8);
        parent.set_max_config_bytes(4_096);
        parent.set_max_sqlite_connections(2);
        parent.set_max_sqlite_statements(5);
        let child = parent.new_child_config().unwrap();
        parent.validate_child_config(&child).unwrap();

        let mut escalated = child.clone();
        escalated.set_max_modules(5);
        assert!(parent
            .validate_child_config(&escalated)
            .unwrap_err()
            .contains("max_modules"));

        let mut escalated = child.clone();
        escalated.set_max_config_bytes(4_097);
        assert!(parent
            .validate_child_config(&escalated)
            .unwrap_err()
            .contains("max_config_bytes"));

        let mut escalated = child;
        escalated.set_max_sqlite_statements(6);
        assert!(parent
            .validate_child_config(&escalated)
            .unwrap_err()
            .contains("max_sqlite_statements"));

        let mut remote = DefaultProcessConfig::default();
        remote.set_max_module_bytes(DEFAULT_MAX_MODULE_BYTES + 1);
        assert!(remote
            .validate_distributed_config()
            .unwrap_err()
            .contains("receiver ceiling"));

        let mut remote = DefaultProcessConfig::default();
        remote.set_max_sqlite_connections(DEFAULT_MAX_SQLITE_CONNECTIONS + 1);
        assert!(remote
            .validate_distributed_config()
            .unwrap_err()
            .contains("receiver ceiling"));
    }

    #[test]
    fn config_debug_reports_counts_without_secret_values() {
        let config = DefaultProcessConfig {
            command_line_arguments: vec!["argument-sentinel".to_owned()],
            environment_variables: vec![("TOKEN".to_owned(), "secret-sentinel".to_owned())],
            preopened_dirs: vec![("guest".to_owned(), "/private/sentinel".to_owned())],
            ..DefaultProcessConfig::default()
        };

        let debug = format!("{config:?}");
        assert!(debug.contains("argument_count: 1"));
        assert!(debug.contains("environment_variable_count: 1"));
        assert!(debug.contains("preopened_dir_count: 1"));
        assert!(!debug.contains("argument-sentinel"));
        assert!(!debug.contains("secret-sentinel"));
        assert!(!debug.contains("/private/sentinel"));
    }

    #[test]
    fn child_config_is_denied_and_inherits_only_resource_ceilings() {
        let mut parent = DefaultProcessConfig::default();
        parent.set_max_memory(4096);
        parent.set_max_fuel(Some(25));
        parent.set_max_table_elements(64);
        parent.set_max_file_descriptors(8);
        parent.set_max_network_connections(4);
        parent.set_can_compile_modules(true);
        parent.set_can_create_configs(true);
        parent.set_can_spawn_processes(true);
        parent.set_can_use_tls_credential_handles(true);
        parent.preopen_dir(".");
        parent.set_command_line_arguments(vec!["secret-argument".into()]);
        parent.set_environment_variables(vec![("SECRET".into(), "value".into())]);

        let child = parent.new_child_config().unwrap();

        assert!(!child.can_compile_modules());
        assert!(!child.can_create_configs());
        assert!(!child.can_spawn_processes());
        assert!(!child.can_use_tls_credential_handles());
        assert!(child.preopened_dirs().is_empty());
        assert!(child.command_line_arguments().is_empty());
        assert!(child.environment_variables().is_empty());
        assert_eq!(child.get_max_memory(), 4096);
        assert_eq!(child.get_max_fuel(), Some(25));
        assert_eq!(child.get_max_table_elements(), 64);
        assert_eq!(child.get_max_file_descriptors(), 8);
        assert_eq!(child.get_max_network_connections(), 4);
        parent.validate_child_config(&child).unwrap();
    }

    #[test]
    fn validator_rejects_every_capability_and_resource_escalation() {
        let mut parent = DefaultProcessConfig::default();
        parent.set_max_memory(4096);
        parent.set_max_fuel(Some(25));
        parent.set_max_table_elements(64);
        parent.set_max_file_descriptors(8);
        parent.set_max_network_connections(4);
        let child = parent.new_child_config().unwrap();

        let mut candidate = child.clone();
        candidate.set_can_compile_modules(true);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("compile-module capability"));

        let mut candidate = child.clone();
        candidate.set_can_create_configs(true);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("create-config capability"));

        let mut candidate = child.clone();
        candidate.set_can_spawn_processes(true);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("spawn capability"));

        let mut candidate = child.clone();
        candidate.set_can_use_tls_credential_handles(true);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("TLS credential-handle capability"));

        let mut candidate = child.clone();
        candidate.set_max_memory(4097);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("max_memory"));

        let mut candidate = child.clone();
        candidate.set_max_fuel(Some(26));
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("max_fuel"));

        let mut candidate = child.clone();
        candidate.set_max_fuel(None);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("unlimited fuel"));

        let mut candidate = child.clone();
        candidate.set_max_table_elements(65);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("max_table_elements"));

        let mut candidate = child.clone();
        candidate.set_max_file_descriptors(9);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("max_file_descriptors"));

        let mut candidate = child;
        candidate.set_max_network_connections(5);
        assert!(parent
            .validate_child_config(&candidate)
            .unwrap_err()
            .contains("max_network_connections"));
    }

    #[test]
    fn validator_allows_only_filesystem_subsets() {
        let mut parent = DefaultProcessConfig::default();
        parent.preopen_dir("crates");

        let mut allowed = parent.new_child_config().unwrap();
        allowed.preopen_dir("crates/lunatic-process-api");
        parent.validate_child_config(&allowed).unwrap();

        let mut denied = parent.new_child_config().unwrap();
        denied.preopen_dir(".");
        assert!(parent
            .validate_child_config(&denied)
            .unwrap_err()
            .contains("outside parent filesystem authority"));
    }

    #[test]
    fn distributed_validator_rejects_host_local_preopens() {
        let empty = DefaultProcessConfig::default();
        empty.validate_distributed_config().unwrap();

        let mut with_preopen = empty;
        with_preopen.preopen_dir(".");
        assert!(with_preopen
            .validate_distributed_config()
            .unwrap_err()
            .contains("host-local"));
    }

    #[test]
    fn distributed_validator_rejects_zero_queue_and_message_limits() {
        let mut config = DefaultProcessConfig::default();
        config.set_max_mailbox_messages(0);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("max_mailbox_messages"));

        let mut config = DefaultProcessConfig::default();
        config.set_max_signal_queue(0);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("max_signal_queue"));

        let mut config = DefaultProcessConfig::default();
        config.set_max_message_size(0);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("max_message_size"));
    }

    #[test]
    fn distributed_validator_applies_receiver_resource_ceilings() {
        let mut config = DefaultProcessConfig::default();
        config.set_max_memory(config.get_max_memory() + 1);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("receiver ceiling"));

        let mut config = DefaultProcessConfig::default();
        config.set_max_signal_queue(config.get_max_signal_queue() + 1);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("receiver ceiling"));

        let mut config = DefaultProcessConfig::default();
        config.set_max_network_connections(config.get_max_network_connections() + 1);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("receiver ceiling"));
    }

    #[test]
    fn distributed_validator_rejects_receiver_local_capability_escalation() {
        let mut config = DefaultProcessConfig::default();
        config.set_can_compile_modules(true);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("compile-module capability"));

        let mut config = DefaultProcessConfig::default();
        config.set_can_create_configs(true);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("create-config capability"));

        let mut config = DefaultProcessConfig::default();
        config.set_can_spawn_processes(true);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("spawn capability"));

        let mut config = DefaultProcessConfig::default();
        config.set_can_use_tls_credential_handles(true);
        assert!(config
            .validate_distributed_config()
            .unwrap_err()
            .contains("TLS credential-handle capability"));
    }

    #[cfg(unix)]
    #[test]
    fn validator_rejects_symlink_escape_from_preopen() {
        use std::{fs, os::unix::fs::symlink};

        let root = std::env::temp_dir().join(format!("lunatic-preopen-{}", uuid::Uuid::new_v4()));
        let allowed_dir = root.join("allowed");
        let outside_dir = root.join("outside");
        fs::create_dir_all(&allowed_dir).unwrap();
        fs::create_dir_all(&outside_dir).unwrap();
        let escape = allowed_dir.join("escape");
        symlink(&outside_dir, &escape).unwrap();

        let mut parent = DefaultProcessConfig::default();
        parent.preopen_dir(allowed_dir.to_string_lossy());
        let mut child = parent.new_child_config().unwrap();
        child.preopen_dir(escape.to_string_lossy());

        let error = parent.validate_child_config(&child).unwrap_err();
        assert!(error.contains("outside parent filesystem authority"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn test_accessible_paths() {
        let crates = get_absolute_path(Path::new("crates")).unwrap();
        let sqlite = get_absolute_path(Path::new("crates/lunatic-sqlite-api")).unwrap();
        let src = get_absolute_path(Path::new("crates/lunatic-sqlite-api/src")).unwrap();
        let guest_api =
            get_absolute_path(Path::new("crates/lunatic-sqlite-api/src/guest_api")).unwrap();
        // checks
        assert!(path_is_ancestor(&crates, &guest_api));
        assert!(path_is_ancestor(&sqlite, &guest_api));
        assert!(path_is_ancestor(&src, &guest_api));
        assert!(path_is_ancestor(&guest_api, &guest_api));
    }

    #[test]
    fn test_forbidden_paths() {
        let crates = get_absolute_path(Path::new("crates")).unwrap();
        let sqlite = get_absolute_path(Path::new("crates/lunatic-sqlite-api")).unwrap();
        let src = get_absolute_path(Path::new("crates/lunatic-sqlite-api/src")).unwrap();
        let guest_api =
            get_absolute_path(Path::new("crates/lunatic-sqlite-api/src/guest_api")).unwrap();
        // checks that there's no access to any ancestor paths
        assert!(!path_is_ancestor(&guest_api, &crates));
        assert!(!path_is_ancestor(&guest_api, &sqlite));
        assert!(!path_is_ancestor(&guest_api, &src));
    }

    #[test]
    fn test_forbidden_absolute_paths() {
        let src = get_absolute_path(Path::new("crates/lunatic-sqlite-api/src")).unwrap();
        // checks that there's no access to any ancestor paths
        assert!(!path_is_ancestor(&src, Path::new("/")));
        assert!(!path_is_ancestor(&src, Path::new("/etc/passwd")));
    }

    #[test]
    fn normalized_paths() {
        let crates = get_absolute_path(Path::new("crates")).unwrap();
        let src = get_absolute_path(Path::new("crates/lunatic-sqlite-api/src")).unwrap();
        let sneaky_src =
            get_absolute_path(Path::new("crates/lunatic-sqlite-api/src/../src/.")).unwrap();
        let sneaky_path =
            get_absolute_path(Path::new("crates/lunatic-sqlite-api/src/../src/../../")).unwrap();
        assert_eq!(src, normalize_path(&sneaky_src));
        assert_eq!(crates, normalize_path(&sneaky_path));
    }
}
