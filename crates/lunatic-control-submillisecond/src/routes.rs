use std::collections::HashMap;

use lunatic_control::{
    api::{
        ControlUrls, ModuleBytes, ModuleId, NodeStart, NodeStarted, NodesList, Register,
        Registration,
    },
    NodeInfo,
};
use lunatic_log::info;
use submillisecond::extract::Query;

use crate::{
    api::{
        ok, ApiError, ApiResponse, ControlServerExtractor, HostExtractor, JsonExtractor, NodeAuth,
        PathExtractor,
    },
    server::{ControlServerMessages, ControlServerRequests},
};

pub fn register(
    ControlServerExtractor(control): ControlServerExtractor,
    HostExtractor(host): HostExtractor,
    JsonExtractor(reg): JsonExtractor<Register>,
) -> ApiResponse<Registration> {
    info!("Registration for node name {}", reg.node_name);

    let cert_pem = control.sign_node(reg.csr_pem.clone());

    let mut authentication_token = [0u8; 32];
    getrandom::getrandom(&mut authentication_token).map_err(|err| {
        ApiError::log_internal_err("Error generating random token for registration", err)
    })?;
    let authentication_token = base64_url::encode(&authentication_token);

    ControlServerMessages::register(
        &control,
        reg.clone(),
        cert_pem.clone(),
        authentication_token.clone(),
    );

    ok(Registration {
        node_name: reg.node_name,
        cert_pem_chain: vec![cert_pem],
        authentication_token,
        root_cert: control.root_cert(),
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

pub fn node_stopped(
    node_auth: NodeAuth,
    ControlServerExtractor(control): ControlServerExtractor,
) -> ApiResponse<()> {
    info!("Node {} stopped", node_auth.node_name);

    control.stop_node(node_auth.registration_id as u64);

    ok(())
}

pub fn node_started(
    node_auth: NodeAuth,
    ControlServerExtractor(control): ControlServerExtractor,
    JsonExtractor(data): JsonExtractor<NodeStart>,
) -> ApiResponse<NodeStarted> {
    let (node_id, _node_address) = control.start_node(node_auth.registration_id as u64, data);

    info!("Node {} started with id {}", node_auth.node_name, node_id);

    // TODO spawn all modules on node

    ok(NodeStarted {
        node_id: node_id as i64,
    })
}

pub fn list_nodes(
    _node_auth: NodeAuth,
    Query(query): Query<HashMap<String, String>>,
    ControlServerExtractor(control): ControlServerExtractor,
) -> ApiResponse<NodesList> {
    let nodes = active_nodes(control.get_nodes(), control.get_registrations(), &query);

    ok(NodesList { nodes })
}

fn active_nodes(
    nodes: HashMap<u64, crate::server::NodeDetails>,
    registrations: HashMap<u64, crate::server::Registered>,
    query: &HashMap<String, String>,
) -> Vec<NodeInfo> {
    let mut nodes: Vec<_> = nodes
        .into_iter()
        .filter(|(_, node)| node.status < 2 && !node.node_address.is_empty())
        .filter(|(_, node)| {
            query
                .iter()
                .all(|(key, value)| node.attributes.get(key) == Some(value))
        })
        .filter_map(|(node_id, node)| {
            let registration = registrations.get(&node.registration_id)?;
            Some(NodeInfo {
                id: node_id,
                address: node.node_address.parse().unwrap(),
                name: registration.node_name.to_string(),
            })
        })
        .collect();
    nodes.sort_unstable_by_key(|node| node.id);

    nodes
}

pub fn add_module(
    body: Vec<u8>,
    node_auth: NodeAuth,
    ControlServerExtractor(control): ControlServerExtractor,
) -> ApiResponse<ModuleId> {
    info!("Node {} add_module", node_auth.node_name);

    let module_id = control.add_module(body);
    ok(ModuleId { module_id })
}

pub fn get_module(
    node_auth: NodeAuth,
    PathExtractor(id): PathExtractor<u64>,
    ControlServerExtractor(control): ControlServerExtractor,
) -> ApiResponse<ModuleBytes> {
    info!("Node {} get_module {}", node_auth.node_name, id);

    let all_modules = control.get_modules();
    let bytes = all_modules
        .into_iter()
        .find(|(k, _)| k == &id)
        .map(|(_, m)| m)
        .ok_or_else(|| ApiError::custom_code("error_reading_bytes"))?;

    ok(ModuleBytes { bytes })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use lunatic_control::api::NodeStart;

    use super::active_nodes;
    use crate::server::{start_node_record, stop_node_records, Registered};

    fn node_start(address: &str) -> NodeStart {
        NodeStart {
            node_address: address.parse().unwrap(),
            attributes: HashMap::new(),
        }
    }

    #[test]
    fn restart_lists_the_current_node_id_then_stop_removes_it() {
        let registration_id = 7;
        let mut next_node_id = 41;
        let mut nodes = HashMap::new();
        let (old_node_id, _, retired) = start_node_record(
            &mut nodes,
            &mut next_node_id,
            registration_id,
            node_start("127.0.0.1:3001"),
        );
        assert!(retired.is_empty());
        let (new_node_id, _, retired) = start_node_record(
            &mut nodes,
            &mut next_node_id,
            registration_id,
            node_start("127.0.0.1:3002"),
        );
        assert_eq!(retired, vec![old_node_id]);

        let mut registrations = HashMap::new();
        registrations.insert(
            registration_id,
            Registered {
                node_name: uuid::Uuid::nil(),
                csr_pem: String::new(),
                cert_pem: String::new(),
                auth_token: String::new(),
            },
        );

        let listed = active_nodes(nodes.clone(), registrations.clone(), &HashMap::new());

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, new_node_id);
        assert_ne!(listed[0].id, registration_id);
        assert!(nodes[&old_node_id].status >= 2);

        assert_eq!(
            stop_node_records(&mut nodes, registration_id),
            vec![new_node_id]
        );
        assert!(active_nodes(nodes.clone(), registrations, &HashMap::new()).is_empty());
        assert!(stop_node_records(&mut nodes, registration_id).is_empty());
    }
}
