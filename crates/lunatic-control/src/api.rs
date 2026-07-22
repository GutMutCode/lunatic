use std::{collections::HashMap, fmt, net::SocketAddr};

use serde::{Deserialize, Serialize};

use crate::NodeInfo;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Register {
    pub node_name: uuid::Uuid,
    pub csr_pem: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Registration {
    pub node_name: uuid::Uuid,
    pub cert_pem_chain: Vec<String>,
    pub authentication_token: String,
    pub root_cert: String,
    pub urls: ControlUrls,
    pub envs: Vec<i64>,
    pub is_privileged: bool,
}

impl fmt::Debug for Registration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registration")
            .field("node_name", &self.node_name)
            .field("cert_pem_chain_count", &self.cert_pem_chain.len())
            .field("authentication_token", &"[REDACTED]")
            .field("root_cert", &"[REDACTED]")
            .field("urls", &self.urls)
            .field("envs", &self.envs)
            .field("is_privileged", &self.is_privileged)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlUrls {
    pub api_base: String,
    pub nodes: String,
    pub node_started: String,
    pub node_stopped: String,
    pub get_module: String,
    pub add_module: String,
    pub get_nodes: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStart {
    pub node_address: SocketAddr,
    pub attributes: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStarted {
    // TODO u64 ids should be JSON string but parsed into u64?
    pub node_id: i64,
    /// Certificate chain reissued for the newly allocated node identity.
    ///
    /// The default keeps response parsing compatible with older control
    /// servers; distributed clients deliberately reject an empty chain.
    #[serde(default)]
    pub cert_pem_chain: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodesList {
    pub nodes: Vec<NodeInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModuleBytes {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddModule {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModuleId {
    pub module_id: u64,
}

#[cfg(test)]
mod tests {
    use super::{ControlUrls, Registration};

    #[test]
    fn registration_debug_redacts_credentials() {
        let registration = Registration {
            node_name: uuid::Uuid::nil(),
            cert_pem_chain: vec!["certificate-sentinel".to_owned()],
            authentication_token: "token-sentinel".to_owned(),
            root_cert: "root-sentinel".to_owned(),
            urls: ControlUrls {
                api_base: String::new(),
                nodes: String::new(),
                node_started: String::new(),
                node_stopped: String::new(),
                get_module: String::new(),
                add_module: String::new(),
                get_nodes: String::new(),
            },
            envs: Vec::new(),
            is_privileged: false,
        };

        let debug = format!("{registration:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("certificate-sentinel"));
        assert!(!debug.contains("token-sentinel"));
        assert!(!debug.contains("root-sentinel"));
    }
}
