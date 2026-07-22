use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Query},
    routing::{get, post},
    Extension, Router,
};
use lunatic_control::{api::*, NodeInfo};
use lunatic_distributed::{control::cert::sign_node_certificate, CertAttrs};
use tower_http::limit::RequestBodyLimitLayer;

use crate::{
    api::{
        ok, ok_secret, ApiError, ApiResponse, JsonExtractor, NodeAuth, PathExtractor,
        SecretApiResponse,
    },
    server::ControlServer,
};

pub async fn register(
    control: Extension<Arc<ControlServer>>,
    JsonExtractor(reg): JsonExtractor<Register>,
) -> SecretApiResponse<RegistrationResponse> {
    log::info!("Registration for node name {}", reg.node_name);

    let control = control.as_ref();

    // Registration proves possession of the private key, but the numeric node
    // identity does not exist until `/started`. This provisional certificate
    // is replaced before it can be used for node-to-node QUIC.
    let node_name = reg.node_name.hyphenated().to_string();
    let cert_pem = sign_node_certificate(
        &reg.csr_pem,
        &control.ca_cert,
        &node_name,
        &CertAttrs {
            node_id: None,
            allowed_envs: vec![],
            is_privileged: true,
        },
    )
    .map_err(|_| ApiError::custom_code("registration_certificate_failed"))?;

    let authentication_token = control
        .register(&reg, &cert_pem)
        .map_err(|_| ApiError::custom_code("registration_bearer_failed"))?;

    ok_secret(RegistrationResponse {
        node_name: reg.node_name,
        cert_pem_chain: vec![cert_pem],
        authentication_token,
        bearer_generation: 0,
        bearer_expires_in_seconds: control.bearer_ttl_seconds(),
        root_cert: control.ca_cert.certificate_pem().to_owned(),
        urls: control.control_urls(),
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
    control
        .stop_node(node_auth.registration_id, node_auth.bearer_generation)
        .map_err(|_| ApiError::custom_code("node_stop_failed"))?;

    ok(())
}

pub async fn node_started(
    node_auth: NodeAuth,
    control: Extension<Arc<ControlServer>>,
    JsonExtractor(data): JsonExtractor<NodeStart>,
) -> SecretApiResponse<NodeStarted> {
    let control = control.as_ref();
    let (node_id, _node_address, cert_pem) = control
        .start_node(node_auth.registration_id, node_auth.bearer_generation, data)
        .map_err(|_| ApiError::custom_code("node_start_failed"))?;

    log::info!("Node {} started with id {}", node_auth.node_name, node_id);

    // TODO spawn all modules on node

    ok_secret(NodeStarted {
        node_id: node_id as i64,
        cert_pem_chain: vec![cert_pem],
        bearer_generation: node_auth.bearer_generation,
        bearer_expires_in_seconds: control.bearer_ttl_seconds(),
    })
}

pub async fn node_refreshed(
    node_auth: NodeAuth,
    control: Extension<Arc<ControlServer>>,
    JsonExtractor(data): JsonExtractor<NodeBearerRefresh>,
) -> SecretApiResponse<NodeRefreshed> {
    if data.current_bearer_generation != node_auth.bearer_generation {
        return Err(ApiError::custom_code("node_bearer_generation_mismatch"));
    }
    let next_bearer_generation = control
        .stage_bearer_rotation(
            node_auth.registration_id,
            node_auth.bearer_generation,
            &data.next_authentication_token,
        )
        .map_err(|_| ApiError::custom_code("node_bearer_refresh_failed"))?;
    ok_secret(NodeRefreshed {
        next_bearer_generation,
        bearer_expires_in_seconds: control.bearer_ttl_seconds(),
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
    let module_id = control
        .add_module(
            node_auth.registration_id,
            node_auth.bearer_generation,
            body.to_vec(),
        )
        .map_err(|_| ApiError::custom_code("module_add_failed"))?;
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
        .route("/refreshed", post(node_refreshed))
        .route("/nodes", get(list_nodes))
        .route("/module", post(add_module))
        .route("/module/:id", get(get_module))
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(50 * 1024 * 1024)) // 50 mb
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use lunatic_control::api::{NodeStart, Register};

    use super::active_nodes;
    use crate::server::ControlServer;

    fn test_control_server() -> anyhow::Result<ControlServer> {
        let ca_cert_str = lunatic_distributed::distributed::server::test_root_cert();
        let ca_cert = lunatic_distributed::control::cert::test_root_cert()?;
        let (ctrl_cert, ctrl_pk) =
            lunatic_distributed::control::cert::default_server_certificates(&ca_cert)?;
        let quic_client =
            lunatic_distributed::quic::new_quic_client(&ca_cert_str, &ctrl_cert, &ctrl_pk)?;

        Ok(ControlServer::new(
            ca_cert,
            quic_client,
            "http://127.0.0.1:3030/".to_owned(),
        ))
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
        let node_name = uuid::Uuid::from_u128(7);
        let csr_pem = lunatic_distributed::distributed::server::gen_node_cert(
            &node_name.hyphenated().to_string(),
        )?
        .serialize_request_pem()?;
        control.register(&Register { node_name, csr_pem }, "provisional-certificate")?;
        let registration_id = *control
            .registrations
            .iter()
            .next()
            .expect("registration")
            .key();

        let (old_node_id, _, old_cert) =
            control.start_node(registration_id, 0, node_start("127.0.0.1:3001"))?;
        let (new_node_id, _, new_cert) =
            control.start_node(registration_id, 0, node_start("127.0.0.1:3002"))?;

        let listed = active_nodes(&control, &HashMap::new());
        assert_eq!(
            listed.iter().map(|node| node.id).collect::<Vec<_>>(),
            vec![new_node_id]
        );
        assert!(!control.nodes.contains_key(&old_node_id));
        assert_ne!(old_cert, new_cert);
        assert_eq!(
            control
                .registrations
                .get(&registration_id)
                .unwrap()
                .cert_pem,
            new_cert
        );

        control.stop_node(registration_id, 0)?;

        assert!(active_nodes(&control, &HashMap::new()).is_empty());
        assert!(!control.nodes.contains_key(&new_node_id));
        assert!(!control.registrations.contains_key(&registration_id));
        Ok(())
    }
}
