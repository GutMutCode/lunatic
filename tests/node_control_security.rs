use std::{collections::HashMap, net::TcpListener as StdTcpListener, time::Duration};

use anyhow::{anyhow, Result};
use lunatic_control::api::{
    ControlUrls, NodeBearerRefresh, NodeRefreshed, NodeStarted, NodesList, Register,
    RegistrationResponse, WireBearerToken, DEFAULT_NODE_BEARER_TTL_SECONDS,
};
use lunatic_distributed::control;
use reqwest::{header, redirect, Url};
use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const REDIRECT_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const FORGED_TOKEN: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
const PROXY_TOKEN: &str = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI";

struct EnvironmentRestore {
    values: Vec<(&'static str, Option<std::ffi::OsString>)>,
}

impl EnvironmentRestore {
    fn set_proxy(proxy: &str) -> Self {
        let names = ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"];
        let values = names
            .iter()
            .copied()
            .map(|name| {
                let previous = std::env::var_os(name);
                std::env::set_var(name, proxy);
                (name, previous)
            })
            .collect();
        Self { values }
    }
}

impl Drop for EnvironmentRestore {
    fn drop(&mut self) {
        for (name, previous) in self.values.drain(..) {
            if let Some(previous) = previous {
                std::env::set_var(name, previous);
            } else {
                std::env::remove_var(name);
            }
        }
    }
}

fn control_urls(base: &str) -> ControlUrls {
    let base = base.trim_end_matches('/');
    ControlUrls {
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

fn registration_response(base: &str, node_name: uuid::Uuid, token: &str) -> RegistrationResponse {
    RegistrationResponse {
        node_name,
        cert_pem_chain: vec!["provisional-certificate".to_owned()],
        authentication_token: WireBearerToken::new(token).unwrap(),
        bearer_generation: 0,
        bearer_expires_in_seconds: DEFAULT_NODE_BEARER_TTL_SECONDS,
        root_cert: "test-root".to_owned(),
        urls: control_urls(base),
        envs: Vec::new(),
        is_privileged: true,
    }
}

async fn read_request(mut stream: TcpStream) -> Result<(TcpStream, String)> {
    let mut request = Vec::new();
    let mut chunk = [0u8; 2048];
    let mut expected_len = None;
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        if expected_len.is_none() {
            if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..header_end + 4]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                    })
                    .unwrap_or(0);
                expected_len = Some(header_end + 4 + content_length);
            }
        }
        if expected_len.is_some_and(|expected| request.len() >= expected) {
            break;
        }
    }
    Ok((stream, String::from_utf8(request)?))
}

async fn respond(
    mut stream: TcpStream,
    status: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<()> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn respond_json(stream: TcpStream, body: &[u8]) -> Result<()> {
    respond(
        stream,
        "200 OK",
        &[
            ("Content-Type", "application/json"),
            ("Cache-Control", "no-store"),
        ],
        body,
    )
    .await
}

fn request_json<T: DeserializeOwned>(request: &str) -> Result<T> {
    let (_, body) = request
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow!("request body boundary missing"))?;
    serde_json::from_str(body).map_err(Into::into)
}

fn has_bearer(request: &str, bearer: &str) -> bool {
    request
        .to_ascii_lowercase()
        .contains(&format!("authorization: bearer {bearer}").to_ascii_lowercase())
}

async fn redirect_is_not_followed() -> Result<()> {
    let attacker = TcpListener::bind("127.0.0.1:0").await?;
    let attacker_url = format!("http://{}/stolen", attacker.local_addr()?);
    let control_listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}/", control_listener.local_addr()?);
    let node_name = uuid::Uuid::from_u128(101);
    let response = serde_json::to_vec(&registration_response(&base, node_name, REDIRECT_TOKEN))?;

    let server = tokio::spawn(async move {
        let (stream, _) = control_listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("POST / ") {
            return Err(anyhow!("expected registration request"));
        }
        respond_json(stream, &response).await?;

        let (stream, _) = control_listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        let request_lower = request.to_ascii_lowercase();
        if !request.starts_with("POST /started ")
            || !request_lower
                .contains(&format!("authorization: bearer {REDIRECT_TOKEN}").to_ascii_lowercase())
        {
            return Err(anyhow!("expected authenticated start request"));
        }
        respond(
            stream,
            "307 Temporary Redirect",
            &[("Location", attacker_url.as_str())],
            b"",
        )
        .await?;

        let (stream, _) = control_listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("POST /stopped ") {
            return Err(anyhow!("expected best-effort cleanup request"));
        }
        respond_json(stream, b"null").await
    });

    let registration =
        control::Client::register(Url::parse(&base)?, node_name, "test-csr".to_owned()).await?;
    let error =
        match control::Client::new(registration, "127.0.0.1:4101".parse()?, HashMap::new()).await {
            Ok(_) => return Err(anyhow!("redirected start unexpectedly succeeded")),
            Err(error) => error,
        };
    assert_eq!(error.to_string(), "control_start_redirect_forbidden");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), attacker.accept())
            .await
            .is_err(),
        "redirect target received a request"
    );
    server.await??;
    Ok(())
}

async fn forged_endpoint_is_rejected_before_bearer_use() -> Result<()> {
    let attacker = TcpListener::bind("127.0.0.1:0").await?;
    let attacker_base = format!("http://{}/", attacker.local_addr()?);
    let control_listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}/", control_listener.local_addr()?);
    let node_name = uuid::Uuid::from_u128(102);
    let mut response = registration_response(&base, node_name, FORGED_TOKEN);
    response.urls.node_started = format!("{}started", attacker_base);
    let response = serde_json::to_vec(&response)?;

    let server = tokio::spawn(async move {
        let (stream, _) = control_listener.accept().await?;
        let (stream, _) = read_request(stream).await?;
        respond_json(stream, &response).await
    });
    let error = control::Client::register(Url::parse(&base)?, node_name, "test-csr".to_owned())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "control_endpoint_origin_mismatch");
    assert!(!error.to_string().contains(FORGED_TOKEN));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), attacker.accept())
            .await
            .is_err(),
        "forged endpoint received a bearer request"
    );
    server.await??;
    Ok(())
}

async fn oversized_registration_response_is_rejected_before_buffering() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}/", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (mut stream, _) = read_request(stream).await?;
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 4194305\r\nConnection: close\r\n\r\n",
            )
            .await?;
        stream.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    });

    let error = control::Client::register(
        Url::parse(&base)?,
        uuid::Uuid::from_u128(108),
        "test-csr".to_owned(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "control_registration_response_too_large");
    server.await??;
    Ok(())
}

async fn proxy_environment_is_ignored() -> Result<()> {
    let proxy = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_url = format!("http://{}", proxy.local_addr()?);
    let target = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}/", target.local_addr()?);
    let node_name = uuid::Uuid::from_u128(103);
    let response = serde_json::to_vec(&registration_response(&base, node_name, PROXY_TOKEN))?;
    let started = serde_json::to_vec(&NodeStarted {
        node_id: 103,
        cert_pem_chain: vec!["proxy-node-certificate".to_owned()],
        bearer_generation: 0,
        bearer_expires_in_seconds: DEFAULT_NODE_BEARER_TTL_SECONDS,
    })?;
    let nodes = serde_json::to_vec(&NodesList { nodes: Vec::new() })?;
    let target_task = tokio::spawn(async move {
        let (stream, _) = target.accept().await?;
        let (stream, _) = read_request(stream).await?;
        respond_json(stream, &response).await?;

        let (stream, _) = target.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("POST /started ")
            || !request
                .to_ascii_lowercase()
                .contains(&format!("authorization: bearer {PROXY_TOKEN}").to_ascii_lowercase())
        {
            return Err(anyhow!("expected authenticated start request"));
        }
        respond_json(stream, &started).await?;

        let (stream, _) = target.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("GET /nodes ")
            || !request
                .to_ascii_lowercase()
                .contains(&format!("authorization: bearer {PROXY_TOKEN}").to_ascii_lowercase())
        {
            return Err(anyhow!("expected authenticated topology request"));
        }
        respond_json(stream, &nodes).await?;

        let (stream, _) = target.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("POST /stopped ") {
            return Err(anyhow!("expected authenticated stop request"));
        }
        respond_json(stream, b"null").await
    });

    let _restore = EnvironmentRestore::set_proxy(&proxy_url);
    let registration =
        control::Client::register(Url::parse(&base)?, node_name, "test-csr".to_owned()).await?;
    let client =
        control::Client::new(registration, "127.0.0.1:4103".parse()?, HashMap::new()).await?;
    client.shutdown().await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), proxy.accept())
            .await
            .is_err(),
        "environment proxy received node-control traffic"
    );
    target_task.await??;
    Ok(())
}

async fn refresh_response_loss_reuses_the_pending_rotation() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}/", listener.local_addr()?);
    let node_name = uuid::Uuid::from_u128(106);
    let registration =
        serde_json::to_vec(&registration_response(&base, node_name, REDIRECT_TOKEN))?;
    let started = serde_json::to_vec(&NodeStarted {
        node_id: 106,
        cert_pem_chain: vec!["response-loss-certificate".to_owned()],
        bearer_generation: 0,
        bearer_expires_in_seconds: DEFAULT_NODE_BEARER_TTL_SECONDS,
    })?;
    let nodes = serde_json::to_vec(&NodesList { nodes: Vec::new() })?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (stream, _) = read_request(stream).await?;
        respond_json(stream, &registration).await?;

        let (stream, _) = listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("POST /started ") || !has_bearer(&request, REDIRECT_TOKEN) {
            return Err(anyhow!("expected start with the current bearer"));
        }
        respond_json(stream, &started).await?;

        let (stream, _) = listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("GET /nodes ") || !has_bearer(&request, REDIRECT_TOKEN) {
            return Err(anyhow!("expected initial topology request"));
        }
        respond_json(stream, &nodes).await?;

        let (stream, _) = listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !has_bearer(&request, REDIRECT_TOKEN) {
            return Err(anyhow!("expected refresh with the current bearer"));
        }
        let first: NodeBearerRefresh = request_json(&request)?;
        let pending = first.next_authentication_token.expose_for_wire().to_owned();
        // Drop the connection without an HTTP response after observing the
        // request, modeling an acknowledgement lost after server staging.
        drop(stream);

        let (stream, _) = listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        let retry: NodeBearerRefresh = request_json(&request)?;
        if !has_bearer(&request, REDIRECT_TOKEN)
            || retry.next_authentication_token.expose_for_wire() != pending
        {
            return Err(anyhow!("rotation retry did not reuse the pending bearer"));
        }
        let refreshed = serde_json::to_vec(&NodeRefreshed {
            next_bearer_generation: 1,
            bearer_expires_in_seconds: DEFAULT_NODE_BEARER_TTL_SECONDS,
        })?;
        respond_json(stream, &refreshed).await?;

        let (stream, _) = listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("GET /nodes ") || !has_bearer(&request, &pending) {
            return Err(anyhow!("acknowledged pending bearer was not installed"));
        }
        respond_json(stream, &nodes).await?;

        let (stream, _) = listener.accept().await?;
        let (stream, request) = read_request(stream).await?;
        if !request.starts_with("POST /stopped ") || !has_bearer(&request, &pending) {
            return Err(anyhow!("expected stop with the promoted bearer"));
        }
        respond_json(stream, b"null").await
    });

    let registration =
        control::Client::register(Url::parse(&base)?, node_name, "test-csr".to_owned()).await?;
    let client =
        control::Client::new(registration, "127.0.0.1:4106".parse()?, HashMap::new()).await?;
    assert!(client.refresh_bearer().await.is_err());
    client.refresh_bearer().await?;
    client.shutdown().await?;
    server.await??;
    Ok(())
}

async fn cancelled_shutdown_still_revokes_the_local_bearer() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}/", listener.local_addr()?);
    let node_name = uuid::Uuid::from_u128(107);
    let registration = serde_json::to_vec(&registration_response(&base, node_name, FORGED_TOKEN))?;
    let started = serde_json::to_vec(&NodeStarted {
        node_id: 107,
        cert_pem_chain: vec!["shutdown-certificate".to_owned()],
        bearer_generation: 0,
        bearer_expires_in_seconds: DEFAULT_NODE_BEARER_TTL_SECONDS,
    })?;
    let nodes = serde_json::to_vec(&NodesList { nodes: Vec::new() })?;
    let (stop_seen_tx, stop_seen_rx) = tokio::sync::oneshot::channel();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (stream, _) = read_request(stream).await?;
        respond_json(stream, &registration).await?;
        let (stream, _) = listener.accept().await?;
        let (stream, _) = read_request(stream).await?;
        respond_json(stream, &started).await?;
        let (stream, _) = listener.accept().await?;
        let (stream, _) = read_request(stream).await?;
        respond_json(stream, &nodes).await?;

        let (stream, _) = listener.accept().await?;
        let (_stream, request) = read_request(stream).await?;
        if !request.starts_with("POST /stopped ") || !has_bearer(&request, FORGED_TOKEN) {
            return Err(anyhow!("expected authenticated stop request"));
        }
        let _ = stop_seen_tx.send(());
        std::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });

    let registration =
        control::Client::register(Url::parse(&base)?, node_name, "test-csr".to_owned()).await?;
    let client =
        control::Client::new(registration, "127.0.0.1:4107".parse()?, HashMap::new()).await?;
    let shutdown_client = client.clone();
    let shutdown = tokio::spawn(async move { shutdown_client.shutdown().await });
    stop_seen_rx.await?;
    shutdown.abort();
    let _ = shutdown.await;
    assert_eq!(
        client.refresh_bearer().await.unwrap_err().to_string(),
        "control_registration_revoked"
    );
    client.shutdown().await?;
    server.abort();
    let _ = server.await;
    Ok(())
}

async fn production_loopback_path_and_host_boundary() -> Result<()> {
    let listener = StdTcpListener::bind("127.0.0.1:0")?;
    let control_addr = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let server = tokio::spawn(async move {
        lunatic_control_axum::server::control_server_from_tcp(listener).await
    });
    let base = format!("http://{control_addr}/");

    let poisoned_name = uuid::Uuid::from_u128(104);
    let poisoned_cert = lunatic_distributed::distributed::server::gen_node_cert(
        &poisoned_name.hyphenated().to_string(),
    )?;
    let response = reqwest::Client::builder()
        .redirect(redirect::Policy::none())
        .no_proxy()
        .build()?
        .post(&base)
        .header(header::HOST, "attacker.example")
        .json(&Register {
            node_name: poisoned_name,
            csr_pem: poisoned_cert.serialize_request_pem()?,
        })
        .send()
        .await?;
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store, max-age=0")
    );
    let issued: RegistrationResponse = response.json().await?;
    assert_eq!(issued.urls.api_base, base);
    assert!(!issued.urls.api_base.contains("attacker.example"));
    drop(issued);

    let node_name = uuid::Uuid::from_u128(105);
    let node_cert = lunatic_distributed::distributed::server::gen_node_cert(
        &node_name.hyphenated().to_string(),
    )?;
    let registration = control::Client::register(
        Url::parse(&base)?,
        node_name,
        node_cert.serialize_request_pem()?,
    )
    .await?;
    assert!(format!("{registration:?}").contains("[REDACTED]"));

    let client = control::Client::new(
        registration,
        "127.0.0.1:4105".parse()?,
        HashMap::from([("security-test".to_owned(), "true".to_owned())]),
    )
    .await?;
    assert_eq!(client.reg().node_name, node_name);
    assert!(!client.reg().cert_pem_chain.is_empty());
    client.refresh_bearer().await?;
    client.refresh_nodes().await?;
    client.shutdown().await?;
    client.shutdown().await?;
    let error = client.refresh_bearer().await.unwrap_err();
    assert_eq!(error.to_string(), "control_registration_revoked");

    server.abort();
    let _ = server.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_control_bearer_security_contract() -> Result<()> {
    redirect_is_not_followed().await?;
    forged_endpoint_is_rejected_before_bearer_use().await?;
    oversized_registration_response_is_rejected_before_buffering().await?;
    proxy_environment_is_ignored().await?;
    refresh_response_loss_reuses_the_pending_rotation().await?;
    cancelled_shutdown_still_revokes_the_local_bearer().await?;
    production_loopback_path_and_host_boundary().await?;
    Ok(())
}
