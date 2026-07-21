use std::{collections::HashMap, sync::Arc};

use asn1_rs::ToDer;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Query},
    routing::{get, post},
    Extension, Json, Router,
};
use lunatic_control::{api::*, NodeInfo};
use lunatic_distributed::{control::cert::TEST_ROOT_CERT, CertAttrs, SUBJECT_DIR_ATTRS};
use rcgen::{CertificateSigningRequestParams, CustomExtension};
use tower_http::limit::RequestBodyLimitLayer;

use crate::{
    api::{ok, ApiError, ApiResponse, HostExtractor, JsonExtractor, NodeAuth, PathExtractor},
    server::ControlServer,
};

pub async fn register(
    control: Extension<Arc<ControlServer>>,
    HostExtractor(host): HostExtractor,
    JsonExtractor(reg): JsonExtractor<Register>,
) -> ApiResponse<Registration> {
    log::info!("Registration for node name {}", reg.node_name);

    let control = control.as_ref();

    let mut sign_request =
        CertificateSigningRequestParams::from_pem(&reg.csr_pem).map_err(|e| {
            ApiError::custom(
                "sign_error",
                format!("Certificate Signing Request invalid pem format: {}", e),
            )
        })?;
    // Add json to custom certificate extension
    sign_request
        .params
        .custom_extensions
        .push(CustomExtension::from_oid_content(
            &SUBJECT_DIR_ATTRS,
            serde_json::to_string(&CertAttrs {
                allowed_envs: vec![],
                is_privileged: true,
            })
            .map_err(|e| ApiError::log_internal("Error serializing allowed envs to JSON", e))?
            .to_der_vec()
            .map_err(|e| ApiError::log_internal("Error serializing allowed envs to der", e))?,
        ));
    let cert_pem = sign_request
        .signed_by(control.ca_cert.issuer())
        .map_err(|e| ApiError::custom("sign_error", e.to_string()))?
        .pem();

    let mut authentication_token = [0u8; 32];
    getrandom::getrandom(&mut authentication_token)
        .map_err(|e| ApiError::log_internal("Error generating random token for registration", e))?;
    let authentication_token = base64_url::encode(&authentication_token);

    control.register(&reg, &cert_pem, &authentication_token);

    ok(Registration {
        node_name: reg.node_name,
        cert_pem_chain: vec![cert_pem],
        authentication_token,
        root_cert: TEST_ROOT_CERT.into(),
        urls: ControlUrls {
            api_base: format!("http://{host}/"),
            nodes: format!("http://{host}/nodes"),
            node_started: format!("http://{host}/started"),
            node_stopped: format!("http://{host}/stopped"),
            get_module: format!("http://{host}/module/{{id}}"),
            add_module: format!("http://{host}/module"),
            get_nodes: format!("http://{host}/nodes"),
        },
        envs: Vec::new(),
        is_privileged: true,
    })
}

pub async fn node_stopped(
    node_auth: NodeAuth,
    control: Extension<Arc<ControlServer>>,
) -> ApiResponse<()> {
    log::info!("Node {} stopped", node_auth.node_name);

    let control = control.as_ref();
    control.stop_node(node_auth.registration_id as u64);

    ok(())
}

pub async fn node_started(
    node_auth: NodeAuth,
    control: Extension<Arc<ControlServer>>,
    Json(data): Json<NodeStart>,
) -> ApiResponse<NodeStarted> {
    let control = control.as_ref();
    let (node_id, _node_address) = control.start_node(node_auth.registration_id as u64, data);

    log::info!("Node {} started with id {}", node_auth.node_name, node_id);

    // TODO spawn all modules on node

    ok(NodeStarted {
        node_id: node_id as i64,
    })
}

pub async fn list_nodes(
    _node_auth: NodeAuth,
    Query(query): Query<HashMap<String, String>>,
    control: Extension<Arc<ControlServer>>,
) -> ApiResponse<NodesList> {
    let control = control.as_ref();
    let nodes = active_nodes(control, &query);

    ok(NodesList { nodes })
}

fn active_nodes(control: &ControlServer, query: &HashMap<String, String>) -> Vec<NodeInfo> {
    let mut nodes: Vec<_> = control
        .nodes
        .iter()
        .filter(|n| n.status < 2 && !n.node_address.is_empty())
        .filter(|node| {
            query
                .iter()
                .all(|(key, value)| node.attributes.get(key) == Some(value))
        })
        .filter_map(|node| {
            let registration = control.registrations.get(&node.registration_id)?;
            Some(NodeInfo {
                id: *node.key(),
                address: node.node_address.parse().unwrap(),
                name: registration.node_name.to_string(),
            })
        })
        .collect();
    nodes.sort_unstable_by_key(|node| node.id);

    nodes
}

pub async fn add_module(
    node_auth: NodeAuth,
    control: Extension<Arc<ControlServer>>,
    body: Bytes,
) -> ApiResponse<ModuleId> {
    log::info!("Node {} add_module", node_auth.node_name);

    let control = control.as_ref();
    let module_id = control.add_module(body.to_vec());
    ok(ModuleId { module_id })
}

pub async fn get_module(
    node_auth: NodeAuth,
    PathExtractor(id): PathExtractor<u64>,
    control: Extension<Arc<ControlServer>>,
) -> ApiResponse<ModuleBytes> {
    log::info!("Node {} get_module {}", node_auth.node_name, id);

    let bytes = control
        .modules
        .iter()
        .find(|m| m.key() == &id)
        .map(|m| m.value().clone())
        .ok_or_else(|| ApiError::custom_code("error_reading_bytes"))?;

    ok(ModuleBytes { bytes })
}

pub fn init_routes() -> Router {
    Router::new()
        .route("/", post(register))
        .route("/stopped", post(node_stopped))
        .route("/started", post(node_started))
        .route("/nodes", get(list_nodes))
        .route("/module", post(add_module))
        .route("/module/:id", get(get_module))
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(50 * 1024 * 1024)) // 50 mb
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use lunatic_control::api::NodeStart;

    use super::active_nodes;
    use crate::server::{ControlServer, Registered};

    fn test_control_server() -> anyhow::Result<ControlServer> {
        let ca_cert_str = lunatic_distributed::distributed::server::test_root_cert();
        let ca_cert = lunatic_distributed::control::cert::test_root_cert()?;
        let (ctrl_cert, ctrl_pk) =
            lunatic_distributed::control::cert::default_server_certificates(&ca_cert)?;
        let quic_client =
            lunatic_distributed::quic::new_quic_client(&ca_cert_str, &ctrl_cert, &ctrl_pk)?;

        Ok(ControlServer::new(ca_cert, quic_client))
    }

    fn node_start(address: &str) -> NodeStart {
        NodeStart {
            node_address: address.parse().unwrap(),
            attributes: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn restarting_then_stopping_lists_only_the_current_node() -> anyhow::Result<()> {
        let control = test_control_server()?;
        let registration_id = 7;
        control.registrations.insert(
            registration_id,
            Registered {
                node_name: uuid::Uuid::nil(),
                csr_pem: String::new(),
                cert_pem: String::new(),
                authentication_token: String::new(),
            },
        );

        let (old_node_id, _) = control.start_node(registration_id, node_start("127.0.0.1:3001"));
        let (new_node_id, _) = control.start_node(registration_id, node_start("127.0.0.1:3002"));

        let listed = active_nodes(&control, &HashMap::new());
        assert_eq!(
            listed.iter().map(|node| node.id).collect::<Vec<_>>(),
            vec![new_node_id]
        );
        assert!(control.nodes.get(&old_node_id).unwrap().status >= 2);

        control.stop_node(registration_id);

        assert!(active_nodes(&control, &HashMap::new()).is_empty());
        assert!(control.nodes.get(&new_node_id).unwrap().status >= 2);
        Ok(())
    }
}
