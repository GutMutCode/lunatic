#[cfg(any())]
mod api;
#[cfg(any())]
mod host;
#[cfg(any())]
mod routes;
#[cfg(any())]
mod server;

#[cfg(any())]
use std::net::ToSocketAddrs;

#[cfg(any())]
use api::RequestBodyLimit;
#[cfg(any())]
use lunatic::AbstractProcess;
#[cfg(any())]
use submillisecond::{router, Application};

#[cfg(any())]
use crate::routes::{add_module, get_module, list_nodes, node_started, node_stopped, register};
#[cfg(any())]
use crate::server::{ControlServer, ControlServerProcess};

fn main() -> anyhow::Result<()> {
    anyhow::bail!(
        "lunatic-control-submillisecond is security-quarantined; use the active Axum control server"
    )
}

// Preserved only as migration source. This path is intentionally not compiled
// until its plaintext bearer persistence and lifecycle contract are replaced.
#[cfg(any())]
fn legacy_main() -> anyhow::Result<()> {
    let root_cert = host::test_root_cert();
    let ca_cert = host::default_server_certificates(&root_cert.cert, &root_cert.pk);

    ControlServer::link()
        .start_as(&ControlServerProcess, ca_cert)
        .unwrap();

    let addrs: Vec<_> = (3030..3999_u16)
        .flat_map(|port| ("127.0.0.1", port).to_socket_addrs().unwrap())
        .collect();

    Application::new(router! {
        with RequestBodyLimit::new(50 * 1024 * 1024); // 50 mb

        POST "/" => register
        POST "/stopped" => node_stopped
        POST "/started" => node_started
        GET "/nodes" => list_nodes
        POST "/module" => add_module
        GET "/module/:id" => get_module
    })
    .serve(addrs.as_slice())?;

    Ok(())
}
