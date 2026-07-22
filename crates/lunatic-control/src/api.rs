use std::{collections::HashMap, fmt, net::SocketAddr};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use zeroize::{Zeroize, Zeroizing};

use crate::NodeInfo;

pub const DEFAULT_NODE_BEARER_TTL_SECONDS: u64 = 5 * 60;
pub const NODE_BEARER_BYTES: usize = 32;
pub const MAX_NODE_BEARER_BYTES: usize = 43;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Register {
    pub node_name: uuid::Uuid,
    pub csr_pem: String,
}

/// Runtime-only bearer secret.
///
/// This type deliberately implements neither `Clone` nor Serde traits. Raw
/// serialization is confined to [`WireBearerToken`], the explicit issuance
/// and rotation envelope used by the control protocol.
pub struct BearerToken(Zeroizing<String>);

impl BearerToken {
    pub fn new(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = Zeroizing::new(value.into());
        if value.len() != MAX_NODE_BEARER_BYTES {
            return Err("invalid node bearer");
        }
        let decoded =
            Zeroizing::new(base64_url::decode(value.as_str()).map_err(|_| "invalid node bearer")?);
        let canonical = Zeroizing::new(base64_url::encode(&decoded));
        if decoded.len() != NODE_BEARER_BYTES || canonical.as_str() != value.as_str() {
            return Err("invalid node bearer");
        }
        Ok(Self(value))
    }

    pub fn expose_for_authorization(&self) -> &str {
        self.0.as_str()
    }

    pub fn revoke(&mut self) {
        self.0.zeroize();
    }

    pub fn is_revoked(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BearerToken([REDACTED])")
    }
}

/// Explicit wire envelope for an issued or client-proposed bearer.
///
/// Unlike [`BearerToken`], this type implements Serde because the raw value
/// must cross a registration response or rotation request. It is
/// non-cloneable, debug-redacted, and zeroizes its allocation on drop.
pub struct WireBearerToken(BearerToken);

impl WireBearerToken {
    pub fn new(value: impl Into<String>) -> Result<Self, &'static str> {
        BearerToken::new(value).map(Self)
    }

    pub fn expose_for_wire(&self) -> &str {
        self.0.expose_for_authorization()
    }

    pub fn generate() -> Result<Self, &'static str> {
        let mut random = Zeroizing::new([0u8; NODE_BEARER_BYTES]);
        getrandom::getrandom(random.as_mut()).map_err(|_| "node bearer issuance failed")?;
        Self::new(base64_url::encode(&random))
    }

    /// Make the runtime copy that a client retains while this wire envelope is
    /// borrowed by an in-flight rotation request.
    pub fn runtime_copy(&self) -> Result<BearerToken, &'static str> {
        BearerToken::new(self.expose_for_wire().to_owned())
    }

    pub fn into_runtime(self) -> BearerToken {
        self.0
    }
}

impl fmt::Debug for WireBearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WireBearerToken([REDACTED])")
    }
}

impl Serialize for WireBearerToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.expose_for_wire())
    }
}

impl<'de> Deserialize<'de> for WireBearerToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Serialize, Deserialize)]
pub struct RegistrationResponse {
    pub node_name: uuid::Uuid,
    pub cert_pem_chain: Vec<String>,
    pub authentication_token: WireBearerToken,
    #[serde(default)]
    pub bearer_generation: u64,
    pub bearer_expires_in_seconds: u64,
    pub root_cert: String,
    pub urls: ControlUrls,
    pub envs: Vec<i64>,
    pub is_privileged: bool,
}

impl fmt::Debug for RegistrationResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistrationResponse")
            .field("node_name", &self.node_name)
            .field("cert_pem_chain_count", &self.cert_pem_chain.len())
            .field("authentication_token", &"[REDACTED]")
            .field("bearer_generation", &self.bearer_generation)
            .field("bearer_expires_in_seconds", &self.bearer_expires_in_seconds)
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
    pub node_refreshed: String,
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

#[derive(Serialize, Deserialize)]
pub struct NodeStarted {
    // TODO u64 ids should be JSON string but parsed into u64?
    pub node_id: i64,
    /// Certificate chain reissued for the newly allocated node identity.
    ///
    /// The default keeps response parsing compatible with older control
    /// servers; distributed clients deliberately reject an empty chain.
    #[serde(default)]
    pub cert_pem_chain: Vec<String>,
    #[serde(default)]
    pub bearer_generation: u64,
    pub bearer_expires_in_seconds: u64,
}

impl fmt::Debug for NodeStarted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeStarted")
            .field("node_id", &self.node_id)
            .field("cert_pem_chain_count", &self.cert_pem_chain.len())
            .field("bearer_generation", &self.bearer_generation)
            .field("bearer_expires_in_seconds", &self.bearer_expires_in_seconds)
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
pub struct NodeBearerRefresh {
    pub current_bearer_generation: u64,
    pub next_authentication_token: WireBearerToken,
}

impl fmt::Debug for NodeBearerRefresh {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeBearerRefresh")
            .field("current_bearer_generation", &self.current_bearer_generation)
            .field("next_authentication_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRefreshed {
    pub next_bearer_generation: u64,
    pub bearer_expires_in_seconds: u64,
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
    use super::{BearerToken, ControlUrls, RegistrationResponse, WireBearerToken};

    const TOKEN_ZERO: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TOKEN_ONE: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";

    #[test]
    fn registration_debug_redacts_credentials() {
        let registration = RegistrationResponse {
            node_name: uuid::Uuid::nil(),
            cert_pem_chain: vec!["certificate-sentinel".to_owned()],
            authentication_token: WireBearerToken::new(TOKEN_ZERO).unwrap(),
            bearer_generation: 0,
            bearer_expires_in_seconds: 300,
            root_cert: "root-sentinel".to_owned(),
            urls: ControlUrls {
                api_base: String::new(),
                nodes: String::new(),
                node_started: String::new(),
                node_refreshed: String::new(),
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
        assert!(!debug.contains(TOKEN_ZERO));
        assert!(!debug.contains("root-sentinel"));
    }

    #[test]
    fn bearer_runtime_and_wire_debug_are_redacted() {
        let runtime = BearerToken::new(TOKEN_ZERO).unwrap();
        let wire = WireBearerToken::new(TOKEN_ONE).unwrap();

        assert!(!format!("{runtime:?}").contains(TOKEN_ZERO));
        assert!(!format!("{wire:?}").contains(TOKEN_ONE));
    }

    #[test]
    fn bearer_values_must_be_canonical_256_bit_base64url() {
        assert!(BearerToken::new("").is_err());
        assert!(BearerToken::new("token-sentinel").is_err());
        assert!(BearerToken::new(format!("{TOKEN_ZERO}=")).is_err());
        assert!(BearerToken::new(TOKEN_ZERO).is_ok());
        assert!(WireBearerToken::generate().is_ok());
    }
}
