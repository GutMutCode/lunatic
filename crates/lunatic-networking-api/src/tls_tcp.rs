use std::convert::TryInto;
use std::future::Future;
use std::io::{self, IoSlice};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::time::timeout;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::rustls::{
    self,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use wasmtime::{Caller, Linker, ToWasmtimeResult as _};

use lunatic_common_api::{audit_log, get_memory, IntoTrap, LinkerAsyncExt};
use lunatic_error_api::ErrorCtx;

use crate::dns::DnsIterator;
use crate::{
    socket_address, validate_memory_range, NetworkingCtx, TlsClientConnectionMetadata,
    TlsConnection, TlsListener,
};

// Register TLS networking APIs to the linker
pub fn register<T: NetworkingCtx + ErrorCtx + Send + 'static>(
    linker: &mut Linker<T>,
) -> Result<()> {
    linker.func_wrap10_async("lunatic::networking", "tls_bind", tls_bind)?;
    linker.func_wrap(
        "lunatic::networking",
        "drop_tls_listener",
        |caller: Caller<'_, T>, tls_listener_id: u64| {
            drop_tls_listener(caller, tls_listener_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::networking",
        "tls_local_addr",
        |caller: Caller<'_, T>, tls_listener_id: u64, id_u64_ptr: u32| {
            tls_local_addr(caller, tls_listener_id, id_u64_ptr).to_wasmtime_result()
        },
    )?;
    linker.func_wrap3_async("lunatic::networking", "tls_accept", tls_accept)?;
    linker.func_wrap7_async("lunatic::networking", "tls_connect", tls_connect)?;
    linker.func_wrap(
        "lunatic::networking",
        "drop_tls_stream",
        |caller: Caller<'_, T>, tls_stream_id: u64| {
            drop_tls_stream(caller, tls_stream_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::networking",
        "clone_tls_stream",
        |caller: Caller<'_, T>, tls_stream_id: u64| {
            clone_tls_stream(caller, tls_stream_id).to_wasmtime_result()
        },
    )?;
    linker.func_wrap4_async(
        "lunatic::networking",
        "tls_write_vectored",
        tls_write_vectored,
    )?;
    linker.func_wrap4_async("lunatic::networking", "tls_read", tls_read)?;
    linker.func_wrap2_async(
        "lunatic::networking",
        "set_tls_read_timeout",
        set_tls_read_timeout,
    )?;
    linker.func_wrap2_async(
        "lunatic::networking",
        "set_tls_write_timeout",
        set_tls_write_timeout,
    )?;
    linker.func_wrap1_async(
        "lunatic::networking",
        "get_tls_read_timeout",
        get_tls_read_timeout,
    )?;
    linker.func_wrap1_async(
        "lunatic::networking",
        "get_tls_write_timeout",
        get_tls_write_timeout,
    )?;
    linker.func_wrap2_async("lunatic::networking", "tls_flush", tls_flush)?;
    Ok(())
}

// Returns the local address that this listener is bound to as an DNS iterator with just one
// element.
// * 0 on success - The local address that this listener is bound to is returned as an DNS
//                  iterator with just one element and written to **id_ptr**.
//
// * 1 on error   - The error ID is written to **id_u64_ptr**
//
// Traps:
// * If the tls listener ID doesn't exist.
// * If any memory outside the guest heap space is referenced.
fn tls_local_addr<T: NetworkingCtx + ErrorCtx>(
    mut caller: Caller<T>,
    tls_listener_id: u64,
    id_u64_ptr: u32,
) -> Result<u32> {
    caller
        .data()
        .tls_listener_resources()
        .get(tls_listener_id)
        .or_trap("lunatic::network::tls_local_addr: listener ID doesn't exist")?;
    let memory = get_memory(&mut caller)?;
    validate_memory_range(
        &caller,
        &memory,
        id_u64_ptr,
        std::mem::size_of::<u64>(),
        "lunatic::network::tls_local_addr",
    )?;
    let lease = caller.data().reserve_dns_iterator_lease();
    let (dns_iter_or_error_id, result) = match lease {
        Ok(lease) => {
            let local_addr = caller
                .data()
                .tls_listener_resources()
                .get(tls_listener_id)
                .expect("validated TLS listener must remain in the resource table")
                .listener
                .local_addr();
            match local_addr {
                Ok(socket_addr) => {
                    let iterator = DnsIterator::with_lease(vec![socket_addr].into_iter(), lease);
                    (caller.data_mut().dns_resources_mut().add(iterator), 0)
                }
                Err(error) => (caller.data_mut().add_error_resource(error.into()), 1),
            }
        }
        Err(error) => (caller.data_mut().add_error_resource(error), 1),
    };

    memory
        .write(
            &mut caller,
            id_u64_ptr as usize,
            &dns_iter_or_error_id.to_le_bytes(),
        )
        .or_trap("lunatic::network::tls_local_addr")?;

    Ok(result)
}

// Creates a new TLS listener, which will be bound to the specified address. The returned listener
// is ready for accepting connections.
//
// Binding with a port number of 0 will request that the OS assigns a port to this listener. The
// port allocated can be queried via the `tls_local_addr` (TODO) method.
//
// Returns:
// * 0 on success - The ID of the newly created TLS listener is written to **id_u64_ptr**
// * 1 on error   - The error ID is written to **id_u64_ptr**
//
// Traps:
// * If any memory outside the guest heap space is referenced.
#[allow(clippy::too_many_arguments)]
fn tls_bind<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    addr_type: u32,
    addr_u8_ptr: u32,
    port: u32,
    flow_info: u32,
    scope_id: u32,
    id_u64_ptr: u32,
    certs_array_ptr: u32,
    certs_array_len: u32,
    keys_array_ptr: u32,
    keys_array_len: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let memory = get_memory(&mut caller)?;
        let certs = memory
            .data(&caller)
            .get(certs_array_ptr as usize..(certs_array_ptr + certs_array_len) as usize)
            .or_trap("lunatic::networking::tls_bind")?
            .to_vec();

        let keys = memory
            .data(&caller)
            .get(keys_array_ptr as usize..(keys_array_ptr + keys_array_len) as usize)
            .or_trap("lunatic::networking::tls_bind")?
            .to_vec();
        let keys = load_private_key(&keys)
            .or_trap("lunatic::networking::tls_bind::failed to unpack the keys")?;
        let certs = load_certs(&certs)
            .or_trap("lunatic::networking::tls_bind::failed to unpack the certs")?;
        let socket_addr = socket_address(
            &caller,
            &memory,
            addr_type,
            addr_u8_ptr,
            port,
            flow_info,
            scope_id,
        )?;
        let lease = caller.data().reserve_network_handle_lease();
        let (tls_listener_or_error_id, result) = match lease {
            Ok(lease) => match TcpListener::bind(socket_addr).await {
                Ok(listener) => {
                    audit_log("tls_bind", format!("address={}", socket_addr));
                    let id = caller
                        .data_mut()
                        .tls_listener_resources_mut()
                        .add(TlsListener {
                            listener,
                            keys,
                            certs,
                        });
                    lease.into_table_reservation();
                    (id, 0)
                }
                Err(error) => (caller.data_mut().add_error_resource(error.into()), 1),
            },
            Err(error) => (caller.data_mut().add_error_resource(error), 1),
        };
        memory
            .write(
                &mut caller,
                id_u64_ptr as usize,
                &tls_listener_or_error_id.to_le_bytes(),
            )
            .or_trap("lunatic::networking::tls_bind::create_environment")?;

        Ok(result)
    })
}

// Drops the TLS listener resource.
//
// Traps:
// * If the TLS listener ID doesn't exist.
fn drop_tls_listener<T: NetworkingCtx>(mut caller: Caller<T>, tls_listener_id: u64) -> Result<()> {
    caller
        .data_mut()
        .tls_listener_resources_mut()
        .remove(tls_listener_id)
        .or_trap("lunatic::networking::drop_tls_listener")?;
    caller.data_mut().release_network_handle()?;
    Ok(())
}

// Returns:
// * 0 on success - The ID of the newly created TLS stream is written to **id_u64_ptr** and the
//                  peer address is returned as an DNS iterator with just one element and written
//                  to **peer_addr_dns_iter_id_u64_ptr**.
// * 1 on error   - The error ID is written to **id_u64_ptr**
//
// Traps:
// * If the tls listener ID doesn't exist.
// * If any memory outside the guest heap space is referenced.
fn tls_accept<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    listener_id: u64,
    id_u64_ptr: u32,
    socket_addr_id_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let (keys, certs) = {
            let tls_listener = caller
                .data()
                .tls_listener_resources()
                .get(listener_id)
                .or_trap("lunatic::network::tls_accept")?;
            (tls_listener.keys.clone_key(), tls_listener.certs.clone())
        };
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certs], keys)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))
            .or_trap("lunatic::network::tls_accept server_config")?;
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let memory = get_memory(&mut caller)?;
        validate_memory_range(
            &caller,
            &memory,
            id_u64_ptr,
            std::mem::size_of::<u64>(),
            "lunatic::networking::tls_accept",
        )?;
        validate_memory_range(
            &caller,
            &memory,
            socket_addr_id_ptr,
            std::mem::size_of::<u64>(),
            "lunatic::networking::tls_accept",
        )?;

        let leases = caller
            .data()
            .reserve_network_handle_lease()
            .and_then(|network| {
                caller
                    .data()
                    .reserve_dns_iterator_lease()
                    .map(|dns| (network, dns))
            });
        let (tls_stream_or_error_id, peer_addr_iter, result) = match leases {
            Ok((network_lease, dns_lease)) => {
                let accept = caller
                    .data()
                    .tls_listener_resources()
                    .get(listener_id)
                    .expect("validated TLS listener must remain in the resource table")
                    .listener
                    .accept()
                    .await;
                match accept {
                    Ok((stream, socket_addr)) => {
                        let stream = acceptor
                            .accept(stream)
                            .await
                            .or_trap("unexpected tls error")?;
                        let stream_id = caller.data_mut().tls_stream_resources_mut().add(Arc::new(
                            TlsConnection::new(tokio_rustls::TlsStream::Server(stream)),
                        ));
                        network_lease.into_table_reservation();
                        let iterator =
                            DnsIterator::with_lease(vec![socket_addr].into_iter(), dns_lease);
                        let dns_iter_id = caller.data_mut().dns_resources_mut().add(iterator);
                        audit_log("tls_accept", format!("peer={}", socket_addr));
                        (stream_id, dns_iter_id, 0)
                    }
                    Err(error) => (caller.data_mut().add_error_resource(error.into()), 0, 1),
                }
            }
            Err(error) => (caller.data_mut().add_error_resource(error), 0, 1),
        };

        memory
            .write(
                &mut caller,
                id_u64_ptr as usize,
                &tls_stream_or_error_id.to_le_bytes(),
            )
            .or_trap("lunatic::networking::tls_accept")?;
        memory
            .write(
                &mut caller,
                socket_addr_id_ptr as usize,
                &peer_addr_iter.to_le_bytes(),
            )
            .or_trap("lunatic::networking::tls_accept")?;
        Ok(result)
    })
}

// Load private key from file.
fn load_private_key(file: &[u8]) -> io::Result<PrivateKeyDer<'static>> {
    let mut reader = io::BufReader::new(file);

    let key = rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| io::Error::other("expected a single private key"))?;
    if rustls_pemfile::private_key(&mut reader)?.is_some() {
        return Err(io::Error::other("expected a single private key"));
    }

    Ok(key)
}

fn load_certs(file: &[u8]) -> io::Result<CertificateDer<'static>> {
    let mut reader = io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader).collect::<io::Result<Vec<_>>>()?;
    if certs.len() != 1 {
        return Err(io::Error::other("expected a single certificate"));
    }

    Ok(certs.into_iter().next().expect("certificate count checked"))
}

// If timeout is specified (value different from `u64::MAX`), the function will return on timeout
// expiration with value 9027.
// If cert_array_len is 0 it is treated as if there's no cert and the default certs are added
//
// Returns:
// * 0 on success - The ID of the newly created TLS stream is written to **id_ptr**.
// * 1 on error   - The error ID is written to **id_ptr**
// * 9027 if the operation timed out
//
// Traps:
// * If **addr_type** is neither 4 or 6.
// * If any memory outside the guest heap space is referenced.
#[allow(clippy::too_many_arguments)]
fn tls_connect<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    addr_str_ptr: u32,
    addr_str_len: u32,
    port: u32,
    timeout_duration: u64,
    id_u64_ptr: u32,
    certs_array_ptr: u32,
    certs_array_len: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let memory = get_memory(&mut caller)?;

        let socket_addr = String::from_utf8(
            memory
                .data(&caller)
                .get(addr_str_ptr as usize..(addr_str_ptr + addr_str_len) as usize)
                .or_trap("lunatic::networking::tls_connect")?
                .to_vec(),
        )
        .or_trap("lunatic::network::tls_connect::tls_connect_socket_addr")?;

        // if cerst_array_len is 0 this means there are no custom certs
        let cafile = if certs_array_len == 0 {
            None
        } else {
            let certs_list = memory
                .data(&caller)
                .get(certs_array_ptr as usize..(certs_array_ptr + certs_array_len * 8) as usize)
                .or_trap("lunatic::networking::tls_connect")?
                .to_vec();

            let vec_slices = certs_list
                .chunks_exact(8)
                .map(|ciovec| {
                    let ciovec_ptr = u32::from_le_bytes(
                        ciovec[0..4]
                            .try_into()
                            .or_trap("lunatic::networking::tls_connect::read_ciovec_ptr")?,
                    ) as usize;
                    let ciovec_len = u32::from_le_bytes(
                        ciovec[4..8]
                            .try_into()
                            .or_trap("lunatic::networking::tls_connect::read_ciovec_len")?,
                    ) as usize;
                    let slice = memory
                        .data(&caller)
                        .get(ciovec_ptr..(ciovec_ptr + ciovec_len))
                        .or_trap("lunatic::networking::tls_connect")?;
                    Ok(slice.to_vec())
                })
                .collect::<Result<Vec<_>>>()?;
            Some(vec_slices)
        };

        let mut root_cert_store = rustls::RootCertStore::empty();
        let custom_certs = if let Some(ref pem_list) = cafile {
            for pem in pem_list {
                let cert =
                    load_certs(pem).or_trap("lunatic::networking::tls_connect::load_certs")?;
                root_cert_store
                    .add(cert)
                    .or_trap("lunatic::networking::tls_connect::load_cert DER")?;
            }
            pem_list.clone()
        } else {
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Vec::new()
        };

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let lease = match caller.data().reserve_network_handle_lease() {
            Ok(lease) => lease,
            Err(error) => {
                let error_id = caller.data_mut().add_error_resource(error);
                memory
                    .write(&mut caller, id_u64_ptr as usize, &error_id.to_le_bytes())
                    .or_trap("lunatic::networking::tls_connect")?;
                return Ok(1);
            }
        };
        let connect = TcpStream::connect((&socket_addr[..], port as u16));
        if let Ok(result) = match timeout_duration {
            // Without timeout
            u64::MAX => Ok(connect.await),
            // With timeout
            t => timeout(Duration::from_millis(t), connect).await,
        } {
            let (stream_or_error_id, result) = match result {
                Ok(tcp_stream) => {
                    let peer_addr = tcp_stream.peer_addr().ok();
                    let local_addr = tcp_stream.local_addr().ok();
                    let domain = ServerName::try_from(socket_addr.clone())
                        .or_trap("lunatic::networking::tls_connect::invalid_dnsname")?;

                    let tls_stream = connector
                        .connect(domain, tcp_stream)
                        .await
                        .or_trap("lunatic::networking::tls_connect::connect failed")?;

                    // Retain descriptive client metadata for diagnostics and
                    // serialized snapshots. In-process hot reload moves the
                    // live stream instead of reconnecting it.
                    let client_metadata = TlsClientConnectionMetadata {
                        server_name: socket_addr.clone(),
                        port: port as u16,
                        peer_addr,
                        local_addr,
                        custom_root_certs: custom_certs,
                    };

                    let id = caller.data_mut().tls_stream_resources_mut().add(Arc::new(
                        TlsConnection::with_client_metadata(
                            TlsStream::Client(tls_stream),
                            client_metadata,
                        ),
                    ));
                    lease.into_table_reservation();
                    audit_log("tls_connect", format!("peer={} port={}", socket_addr, port));
                    (id, 0)
                }
                Err(error) => (caller.data_mut().add_error_resource(error.into()), 1),
            };

            memory
                .write(
                    &mut caller,
                    id_u64_ptr as usize,
                    &stream_or_error_id.to_le_bytes(),
                )
                .or_trap("lunatic::networking::tls_connect")?;
            Ok(result)
        } else {
            // Call timed out
            Ok(9027)
        }
    })
}

// Drops the TLS stream resource..
//
// Traps:
// * If the DNS iterator ID doesn't exist.
fn drop_tls_stream<T: NetworkingCtx>(mut caller: Caller<T>, tls_stream_id: u64) -> Result<()> {
    caller
        .data_mut()
        .tls_stream_resources_mut()
        .remove(tls_stream_id)
        .or_trap("lunatic::networking::drop_tls_stream")?;
    caller.data_mut().release_network_handle()?;
    Ok(())
}

// Clones a TLS stream returning the ID of the clone.
//
// Traps:
// * If the stream ID doesn't exist.
fn clone_tls_stream<T: NetworkingCtx>(mut caller: Caller<T>, tls_stream_id: u64) -> Result<u64> {
    let stream = caller
        .data()
        .tls_stream_resources()
        .get(tls_stream_id)
        .or_trap("lunatic::networking::clone_tls_stream")?
        .clone();
    let lease = caller.data().reserve_network_handle_lease()?;
    let id = caller.data_mut().tls_stream_resources_mut().add(stream);
    lease.into_table_reservation();
    Ok(id)
}

// Gathers data from the vector buffers and writes them to the stream. **ciovec_array_ptr** points
// to an array of (ciovec_ptr, ciovec_len) pairs where each pair represents a buffer to be written.
//
// Returns:
// * 0 on success - The number of bytes written is written to **opaque_ptr**
// * 1 on error   - The error ID is written to **opaque_ptr**
//
// Traps:
// * If the stream ID doesn't exist.
// * If any memory outside the guest heap space is referenced.
fn tls_write_vectored<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    stream_id: u64,
    ciovec_array_ptr: u32,
    ciovec_array_len: u32,
    opaque_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let memory = get_memory(&mut caller)?;
        let buffer = memory
            .data(&caller)
            .get(ciovec_array_ptr as usize..(ciovec_array_ptr + ciovec_array_len * 8) as usize)
            .or_trap("lunatic::networking::tls_write_vectored")?;

        // Ciovecs consist of 32bit ptr + 32bit len = 8 bytes.
        let vec_slices: Result<Vec<_>> = buffer
            .chunks_exact(8)
            .map(|ciovec| {
                let ciovec_ptr = u32::from_le_bytes(
                    ciovec[0..4]
                        .try_into()
                        .or_trap("lunatic::network::tls_write_vectored::ciovec_ptr")?,
                ) as usize;
                let ciovec_len = u32::from_le_bytes(
                    ciovec[4..8]
                        .try_into()
                        .or_trap("lunatic::network::tls_write_vectored::ciovec_ptr")?,
                ) as usize;
                let slice = memory
                    .data(&caller)
                    .get(ciovec_ptr..(ciovec_ptr + ciovec_len))
                    .or_trap("lunatic::networking::tls_write_vectored")?;
                Ok(IoSlice::new(slice))
            })
            .collect();
        let vec_slices = vec_slices?;

        let stream = caller
            .data()
            .tls_stream_resources()
            .get(stream_id)
            .or_trap("lunatic::network::tls_write_vectored")?
            .clone();

        let write_timeout = stream.write_timeout.lock().await;
        let mut stream = stream.writer.lock().await;

        if let Ok(write_result) = match *write_timeout {
            Some(write_timeout) => {
                timeout(write_timeout, stream.write_vectored(vec_slices.as_slice())).await
            }
            None => Ok(stream.write_vectored(vec_slices.as_slice()).await),
        } {
            let (opaque, return_) = match write_result {
                Ok(bytes) => (bytes as u64, 0),
                Err(error) => (caller.data_mut().add_error_resource(error.into()), 1),
            };

            let memory = get_memory(&mut caller)?;
            memory
                .write(&mut caller, opaque_ptr as usize, &opaque.to_le_bytes())
                .or_trap("lunatic::networking::tls_write_vectored")?;
            Ok(return_)
        } else {
            // Call timed out
            Ok(9027)
        }
    })
}

// Sets the new value for write timeout for the **TlsStream**
//
// Returns:
// * 0 on success
//
// Traps:
// * If the stream ID doesn't exist.
fn set_tls_write_timeout<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    stream_id: u64,
    duration: u64,
) -> Box<dyn Future<Output = Result<()>> + Send + '_> {
    Box::new(async move {
        let stream = caller
            .data_mut()
            .tls_stream_resources_mut()
            .get_mut(stream_id)
            .or_trap("lunatic::network::set_tls_write_timeout")?
            .clone();
        let mut timeout = stream.write_timeout.lock().await;
        // a way to disable the timeout
        if duration == u64::MAX {
            *timeout = None;
        } else {
            *timeout = Some(Duration::from_millis(duration));
        }
        Ok(())
    })
}

// Gets the value for write timeout for the **TlsStream**
//
// Returns:
// * value of write timeout duration in milliseconds
//
// Traps:
// * If the stream ID doesn't exist.
fn get_tls_write_timeout<T: NetworkingCtx + ErrorCtx + Send>(
    caller: Caller<T>,
    stream_id: u64,
) -> Box<dyn Future<Output = Result<u64>> + Send + '_> {
    Box::new(async move {
        let stream = caller
            .data()
            .tls_stream_resources()
            .get(stream_id)
            .or_trap("lunatic::network::get_tls_write_timeout")?
            .clone();
        let timeout = stream.write_timeout.lock().await;
        // a way to disable the timeout
        Ok(timeout.map_or(u64::MAX, |t| t.as_millis() as u64))
    })
}

// Sets the new value for write timeout for the **TlsStream**
//
// Returns:
// * 0 on success
//
// Traps:
// * If the stream ID doesn't exist.
pub fn set_tls_read_timeout<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    stream_id: u64,
    duration: u64,
) -> Box<dyn Future<Output = Result<()>> + Send + '_> {
    Box::new(async move {
        let stream = caller
            .data_mut()
            .tls_stream_resources_mut()
            .get_mut(stream_id)
            .or_trap("lunatic::network::set_tls_read_timeout")?
            .clone();
        let mut timeout = stream.read_timeout.lock().await;
        // a way to disable the timeout
        if duration == u64::MAX {
            *timeout = None;
        } else {
            *timeout = Some(Duration::from_millis(duration));
        }
        Ok(())
    })
}

// Gets the value for read timeout for the **TlsStream**
//
// Returns:
// * value of write timeout duration in milliseconds
//
// Traps:
// * If the stream ID doesn't exist.
fn get_tls_read_timeout<T: NetworkingCtx + ErrorCtx + Send>(
    caller: Caller<T>,
    stream_id: u64,
) -> Box<dyn Future<Output = Result<u64>> + Send + '_> {
    Box::new(async move {
        let stream = caller
            .data()
            .tls_stream_resources()
            .get(stream_id)
            .or_trap("lunatic::network::get_tls_read_timeout")?
            .clone();
        let timeout = stream.read_timeout.lock().await;
        // a way to disable the timeout
        Ok(timeout.map_or(u64::MAX, |t| t.as_millis() as u64))
    })
}

// Reads data from TLS stream and writes it to the buffer.
//
// If no data was read within the specified timeout duration the value 9027 is returned
//
// Returns:
// * 0 on success - The number of bytes read is written to **opaque_ptr**
// * 1 on error   - The error ID is written to **opaque_ptr**
//
// Traps:
// * If the stream ID doesn't exist.
// * If any memory outside the guest heap space is referenced.
fn tls_read<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    stream_id: u64,
    buffer_ptr: u32,
    buffer_len: u32,
    opaque_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let stream = caller
            .data()
            .tls_stream_resources()
            .get(stream_id)
            .or_trap("lunatic::network::tls_read")?
            .clone();
        let read_timeout = stream.read_timeout.lock().await;
        let mut stream = stream.reader.lock().await;

        let memory = get_memory(&mut caller)?;
        let buffer = memory
            .data_mut(&mut caller)
            .get_mut(buffer_ptr as usize..(buffer_ptr + buffer_len) as usize)
            .or_trap("lunatic::networking::tls_read")?;

        if let Ok(read_result) = match *read_timeout {
            Some(read_timeout) => timeout(read_timeout, stream.read(buffer)).await,
            None => Ok(stream.read(buffer).await),
        } {
            let (opaque, return_) = match read_result {
                Ok(bytes) => (bytes as u64, 0),
                Err(error) => (caller.data_mut().add_error_resource(error.into()), 1),
            };

            let memory = get_memory(&mut caller)?;
            memory
                .write(&mut caller, opaque_ptr as usize, &opaque.to_le_bytes())
                .or_trap("lunatic::networking::tls_read")?;
            Ok(return_)
        } else {
            // Call timed out
            Ok(9027)
        }
    })
}

// Flushes this output stream, ensuring that all intermediately buffered contents reach their
// destination.
//
// Returns:
// * 0 on success
// * 1 on error   - The error ID is written to **error_id_ptr**
//
// Traps:
// * If the stream ID doesn't exist.
// * If any memory outside the guest heap space is referenced.
fn tls_flush<T: NetworkingCtx + ErrorCtx + Send>(
    mut caller: Caller<T>,
    stream_id: u64,
    error_id_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_> {
    Box::new(async move {
        let stream = caller
            .data()
            .tls_stream_resources()
            .get(stream_id)
            .or_trap("lunatic::network::tls_flush")?
            .clone();

        let mut stream = stream.writer.lock().await;

        let (error_id, result) = match stream.flush().await {
            Ok(()) => (0, 0),
            Err(error) => (caller.data_mut().add_error_resource(error.into()), 1),
        };

        let memory = get_memory(&mut caller)?;
        memory
            .write(&mut caller, error_id_ptr as usize, &error_id.to_le_bytes())
            .or_trap("lunatic::networking::tls_flush")?;
        Ok(result)
    })
}

#[cfg(test)]
mod tests {
    use super::{load_certs, load_private_key};

    const CERT_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n";
    const KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----\nMAMCAQE=\n-----END PRIVATE KEY-----\n";

    #[test]
    fn pem_loaders_accept_exactly_one_item() {
        let cert = load_certs(CERT_PEM).expect("one certificate should load");
        assert_eq!(cert.as_ref(), &[1, 2, 3]);

        let key = load_private_key(KEY_PEM).expect("one private key should load");
        assert_eq!(key.secret_der(), &[0x30, 0x03, 0x02, 0x01, 0x01]);
    }

    #[test]
    fn pem_loaders_reject_empty_input() {
        assert!(load_certs(&[]).is_err());
        assert!(load_private_key(&[]).is_err());
    }

    #[test]
    fn pem_loaders_reject_multiple_items() {
        let certs = [CERT_PEM, CERT_PEM].concat();
        assert!(load_certs(&certs).is_err());

        let keys = [KEY_PEM, KEY_PEM].concat();
        assert!(load_private_key(&keys).is_err());
    }
}
