use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use lunatic_distributed::{
    control::cert,
    distributed::{
        message::{self, Request},
        server::gen_node_cert,
    },
    quic::{
        handle_request_stream, handle_request_stream_with_timeout, new_quic_client,
        new_quic_server, write_message,
    },
};
use tokio::sync::oneshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_quic_framing_dispatches_a_multi_chunk_message() -> Result<()> {
    let root = cert::test_root_cert()?;
    let ca_pem = root.certificate_pem().to_owned();
    let (server_cert, server_key) = cert::default_server_certificates(&root)?;
    let node_cert = gen_node_cert("transport-test-node")?;
    let node_cert_pem = node_cert.serialize_pem_with_signer(&root)?;
    let node_key_pem = node_cert.serialize_private_key_pem();

    let server_addr: SocketAddr = if cfg!(windows) {
        "[::1]:0".parse()?
    } else {
        "127.0.0.1:0".parse()?
    };
    let server = new_quic_server(
        server_addr,
        vec![server_cert, ca_pem.clone()],
        &server_key,
        &ca_pem,
    )?;
    let listen_addr = server.local_addr()?;
    let (dispatch_tx, dispatch_rx) = oneshot::channel();

    let server_task = tokio::spawn(async move {
        let connecting = server.accept().await.context("server endpoint closed")?;
        let connection = connecting.await?;
        let recv = connection.accept_uni().await?;
        let mut dispatch_tx = Some(dispatch_tx);
        handle_request_stream(recv, move |message_id, request| {
            let dispatch_tx = dispatch_tx.take();
            async move {
                if let Some(dispatch_tx) = dispatch_tx {
                    let _ = dispatch_tx.send((message_id, request));
                }
            }
        })
        .await;
        Ok::<_, anyhow::Error>(())
    });

    let client = new_quic_client(&ca_pem, &node_cert_pem, &node_key_pem)?;
    let connection = client._connect(listen_addr, cert::CTRL_SERVER_NAME).await?;
    let mut send = connection.open_uni().await?;
    let payload = (0..4_097).map(|value| (value % 251) as u8).collect();
    let request = Request::Message {
        node_id: 7,
        environment_id: 11,
        process_id: 13,
        tag: Some(17),
        data: payload,
    };
    write_message(&mut send, 23, message::serialize_message(&request)?.into()).await?;
    send.finish()?;

    let (message_id, dispatched) =
        tokio::time::timeout(Duration::from_secs(5), dispatch_rx).await??;
    assert_eq!(message_id, 23);
    assert_eq!(
        message::serialize_message(&dispatched)?,
        message::serialize_message(&request)?
    );

    connection.close(0u32.into(), b"test complete");
    tokio::time::timeout(Duration::from_secs(5), server_task).await???;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_transport_rejects_reverse_streams_and_datagrams() -> Result<()> {
    let root = cert::test_root_cert()?;
    let ca_pem = root.certificate_pem().to_owned();
    let (server_cert, server_key) = cert::default_server_certificates(&root)?;
    let node_cert = gen_node_cert("outbound-only-client-test-node")?;
    let node_cert_pem = node_cert.serialize_pem_with_signer(&root)?;
    let node_key_pem = node_cert.serialize_private_key_pem();

    let server_addr: SocketAddr = if cfg!(windows) {
        "[::1]:0".parse()?
    } else {
        "127.0.0.1:0".parse()?
    };
    let server = new_quic_server(
        server_addr,
        vec![server_cert, ca_pem.clone()],
        &server_key,
        &ca_pem,
    )?;
    let listen_addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connecting = server.accept().await.context("server endpoint closed")?;
        let connection = connecting.await?;
        let reverse_uni_blocked =
            tokio::time::timeout(Duration::from_millis(100), connection.open_uni())
                .await
                .is_err();
        let reverse_bidi_blocked =
            tokio::time::timeout(Duration::from_millis(100), connection.open_bi())
                .await
                .is_err();
        let datagrams_disabled = matches!(
            connection.send_datagram(bytes::Bytes::from_static(b"not accepted")),
            Err(quinn::SendDatagramError::UnsupportedByPeer | quinn::SendDatagramError::Disabled)
        );
        Ok::<_, anyhow::Error>((
            reverse_uni_blocked,
            reverse_bidi_blocked,
            datagrams_disabled,
        ))
    });

    let client = new_quic_client(&ca_pem, &node_cert_pem, &node_key_pem)?;
    let connection = client._connect(listen_addr, cert::CTRL_SERVER_NAME).await?;
    let (reverse_uni_blocked, reverse_bidi_blocked, datagrams_disabled) =
        tokio::time::timeout(Duration::from_secs(5), server_task).await???;
    assert!(reverse_uni_blocked);
    assert!(reverse_bidi_blocked);
    assert!(datagrams_disabled);

    connection.close(0u32.into(), b"test complete");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malicious_chunk_length_is_rejected_before_its_body_arrives() -> Result<()> {
    let root = cert::test_root_cert()?;
    let ca_pem = root.certificate_pem().to_owned();
    let (server_cert, server_key) = cert::default_server_certificates(&root)?;
    let node_cert = gen_node_cert("malicious-frame-test-node")?;
    let node_cert_pem = node_cert.serialize_pem_with_signer(&root)?;
    let node_key_pem = node_cert.serialize_private_key_pem();

    let server_addr: SocketAddr = if cfg!(windows) {
        "[::1]:0".parse()?
    } else {
        "127.0.0.1:0".parse()?
    };
    let server = new_quic_server(
        server_addr,
        vec![server_cert, ca_pem.clone()],
        &server_key,
        &ca_pem,
    )?;
    let listen_addr = server.local_addr()?;
    let dispatched = Arc::new(AtomicBool::new(false));
    let dispatched_by_server = dispatched.clone();

    let server_task = tokio::spawn(async move {
        let connecting = server.accept().await.context("server endpoint closed")?;
        let connection = connecting.await?;
        let recv = connection.accept_uni().await?;
        handle_request_stream(recv, move |_message_id, _request| {
            dispatched_by_server.store(true, Ordering::Relaxed);
            async {}
        })
        .await;
        Ok::<_, anyhow::Error>(())
    });

    let client = new_quic_client(&ca_pem, &node_cert_pem, &node_key_pem)?;
    let connection = client._connect(listen_addr, cert::CTRL_SERVER_NAME).await?;
    let mut send = connection.open_uni().await?;
    let mut malicious_header = Vec::with_capacity(24);
    malicious_header.extend_from_slice(&1u64.to_le_bytes());
    malicious_header.extend_from_slice(&1u32.to_le_bytes());
    malicious_header.extend_from_slice(&0u64.to_le_bytes());
    malicious_header.extend_from_slice(&u32::MAX.to_le_bytes());
    send.write_all(&malicious_header).await?;

    // Keep the send stream open and omit the claimed body. A receiver that trusts chunk_size
    // either attempts a multi-gigabyte allocation or blocks reading it; the hardened receiver
    // rejects the header immediately.
    tokio::time::timeout(Duration::from_secs(2), server_task).await???;
    assert!(!dispatched.load(Ordering::Relaxed));

    connection.close(0u32.into(), b"test complete");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_incomplete_header_and_partial_message_streams_time_out() -> Result<()> {
    let root = cert::test_root_cert()?;
    let ca_pem = root.certificate_pem().to_owned();
    let (server_cert, server_key) = cert::default_server_certificates(&root)?;
    let node_cert = gen_node_cert("idle-frame-test-node")?;
    let node_cert_pem = node_cert.serialize_pem_with_signer(&root)?;
    let node_key_pem = node_cert.serialize_private_key_pem();

    let server_addr: SocketAddr = if cfg!(windows) {
        "[::1]:0".parse()?
    } else {
        "127.0.0.1:0".parse()?
    };
    let server = new_quic_server(
        server_addr,
        vec![server_cert, ca_pem.clone()],
        &server_key,
        &ca_pem,
    )?;
    let listen_addr = server.local_addr()?;
    let dispatched = Arc::new(AtomicBool::new(false));
    let dispatched_by_server = dispatched.clone();

    let server_task = tokio::spawn(async move {
        let connecting = server.accept().await.context("server endpoint closed")?;
        let connection = connecting.await?;
        for _ in 0..2 {
            let recv = connection.accept_uni().await?;
            let dispatched = dispatched_by_server.clone();
            handle_request_stream_with_timeout(
                recv,
                Duration::from_millis(100),
                move |_message_id, _request| {
                    dispatched.store(true, Ordering::Relaxed);
                    async {}
                },
            )
            .await;
        }
        Ok::<_, anyhow::Error>(())
    });

    let client = new_quic_client(&ca_pem, &node_cert_pem, &node_key_pem)?;
    let connection = client._connect(listen_addr, cert::CTRL_SERVER_NAME).await?;

    // A single byte makes the first stream observable to the peer, but can never complete the
    // 24-byte header. Keep the stream open so only the read deadline can release the handler.
    let mut incomplete_header = connection.open_uni().await?;
    incomplete_header.write_all(&[0xAA]).await?;

    // The second stream delivers a valid first chunk for a two-chunk message and then remains
    // open. This exercises timeout cleanup after declared-size budget and buffer admission.
    let mut partial_message = connection.open_uni().await?;
    let mut first_chunk = Vec::with_capacity(24 + 1024);
    first_chunk.extend_from_slice(&2u64.to_le_bytes());
    first_chunk.extend_from_slice(&2048u32.to_le_bytes());
    first_chunk.extend_from_slice(&0u64.to_le_bytes());
    first_chunk.extend_from_slice(&1024u32.to_le_bytes());
    first_chunk.resize(24 + 1024, 0xBB);
    partial_message.write_all(&first_chunk).await?;

    tokio::time::timeout(Duration::from_secs(2), server_task).await???;
    assert!(!dispatched.load(Ordering::Relaxed));

    connection.close(0u32.into(), b"test complete");
    Ok(())
}

#[test]
fn invalid_quic_pem_inputs_return_errors() -> Result<()> {
    let addr = "127.0.0.1:0".parse()?;
    let root = cert::test_root_cert()?;
    let ca_pem = root.certificate_pem();
    let key_pem = root.private_key_pem();

    assert!(new_quic_client("", "", "").is_err());
    assert!(new_quic_client("not a PEM certificate", "", "").is_err());
    assert!(new_quic_client(ca_pem, "", key_pem).is_err());
    assert!(new_quic_client(ca_pem, ca_pem, "not a PEM key").is_err());
    assert!(new_quic_server(addr, Vec::new(), "", "").is_err());
    assert!(new_quic_server(addr, vec![ca_pem.to_owned()], "not a PEM key", ca_pem,).is_err());
    assert!(new_quic_server(
        addr,
        vec!["not a PEM certificate".to_owned()],
        key_pem,
        ca_pem,
    )
    .is_err());

    Ok(())
}

#[test]
fn duplicate_quic_pem_items_return_errors() -> Result<()> {
    let root = cert::test_root_cert()?;
    let ca_pem = root.certificate_pem();
    let node_cert = gen_node_cert("duplicate-pem-test-node")?;
    let node_cert_pem = node_cert.serialize_pem_with_signer(&root)?;
    let node_key_pem = node_cert.serialize_private_key_pem();

    let duplicate_certificates = format!("{ca_pem}{ca_pem}");
    assert!(new_quic_client(&duplicate_certificates, &node_cert_pem, &node_key_pem).is_err());

    let duplicate_keys = format!("{node_key_pem}{node_key_pem}");
    assert!(new_quic_client(ca_pem, &node_cert_pem, &duplicate_keys).is_err());

    Ok(())
}
