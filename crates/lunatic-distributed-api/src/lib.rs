use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use asn1_rs::ToDer;
use lunatic_common_api::{
    emit_audit_event, get_memory, write_to_guest_vec, AuditAction, AuditEvent, AuditEventV1,
    AuditReason, AuditResult, AuditSubject, AuditTarget, AuditTargetKind, IntoTrap, LinkerAsyncExt,
    SensitiveData,
};
use lunatic_distributed::{
    control::cert::CertificateAuthority,
    distributed::{
        self,
        client::{EnvironmentId, NodeId, ProcessId, SendErrorKind, SendParams, SpawnParams},
        message::{ClientError, Spawn, Val},
    },
    CertAttrs, DistributedCtx, SUBJECT_DIR_ATTRS,
};
use lunatic_error_api::ErrorCtx;
use lunatic_process::{config::ProcessConfig, env::Environment, message::Message};
use lunatic_process_api::ProcessCtx;
use rcgen::{CertificateSigningRequestParams, CustomExtension};
use tokio::time::timeout;
use wasmtime::{Caller, Linker, ResourceLimiter, ToWasmtimeResult as _};

struct PendingDistributedAudit {
    event: Option<AuditEvent>,
    action: AuditAction,
    subject: AuditSubject,
    target: AuditTarget,
    fallback_result: AuditResult,
    fallback_reason: AuditReason,
}

impl PendingDistributedAudit {
    fn new(
        event: AuditEvent,
        action: AuditAction,
        subject: AuditSubject,
        target: AuditTarget,
        fallback_reason: AuditReason,
    ) -> Self {
        Self {
            event: Some(event),
            action,
            subject,
            target,
            fallback_result: AuditResult::Failed,
            fallback_reason,
        }
    }

    fn mark_async(&mut self) {
        self.fallback_result = AuditResult::Cancelled;
        self.fallback_reason = AuditReason::Cancelled;
    }

    fn finish(mut self, result: AuditResult, reason: AuditReason) {
        self.emit(result, reason, None);
    }

    fn finish_with_target(mut self, result: AuditResult, reason: AuditReason, target: AuditTarget) {
        self.emit(result, reason, Some(target));
    }

    fn emit(&mut self, result: AuditResult, reason: AuditReason, target: Option<AuditTarget>) {
        if let Some(event) = self.event.take() {
            emit_audit_event(AuditEventV1::new(
                event,
                self.action,
                result,
                reason,
                self.subject,
                target.unwrap_or(self.target),
            ));
        }
    }
}

impl Drop for PendingDistributedAudit {
    fn drop(&mut self) {
        if self.event.is_some() {
            self.emit(self.fallback_result, self.fallback_reason, None);
        }
    }
}

// Register the lunatic distributed APIs to the linker
pub fn register<T, E>(linker: &mut Linker<T>) -> Result<()>
where
    T: DistributedCtx<E> + ProcessCtx<T> + Send + ResourceLimiter + ErrorCtx + 'static,
    E: Environment + 'static,
    for<'a> &'a T: Send,
{
    linker.func_wrap("lunatic::distributed", "nodes_count", nodes_count)?;
    linker.func_wrap(
        "lunatic::distributed",
        "get_nodes",
        |caller: Caller<T>, nodes_ptr: u32, nodes_len: u32| {
            get_nodes::<T, E>(caller, nodes_ptr, nodes_len).to_wasmtime_result()
        },
    )?;
    linker.func_wrap("lunatic::distributed", "node_id", node_id)?;
    linker.func_wrap("lunatic::distributed", "module_id", module_id)?;
    linker.func_wrap8_async("lunatic::distributed", "spawn", spawn)?;
    linker.func_wrap2_async("lunatic::distributed", "send", send)?;
    linker.func_wrap4_async(
        "lunatic::distributed",
        "send_receive_skip_search",
        send_receive_skip_search,
    )?;
    linker.func_wrap5_async(
        "lunatic::distributed",
        "exec_lookup_nodes",
        exec_lookup_nodes,
    )?;
    linker.func_wrap(
        "lunatic::distributed",
        "copy_lookup_nodes_results",
        |caller: Caller<T>, query_id: u64, nodes_ptr: u32, nodes_len: u32, error_ptr: u32| {
            copy_lookup_nodes_results::<T, E>(caller, query_id, nodes_ptr, nodes_len, error_ptr)
                .to_wasmtime_result()
        },
    )?;
    linker.func_wrap1_async("lunatic::distributed", "test_root_cert", test_root_cert)?;
    linker.func_wrap5_async(
        "lunatic::distributed",
        "default_server_certificates",
        default_server_certificates,
    )?;
    linker.func_wrap7_async("lunatic::distributed", "sign_node", sign_node)?;
    Ok(())
}

// Returns the number of registered nodes
fn nodes_count<T, E>(caller: Caller<T>) -> u32
where
    T: DistributedCtx<E>,
    E: Environment,
{
    caller
        .data()
        .distributed()
        .map(|d| d.control.node_count())
        .unwrap_or(0) as u32
}

// Copy node ids into guest memory. Returns the number of nodes copied.
//
// Traps:
// * If any memory outside the guest heap space is referenced.
fn get_nodes<T, E>(mut caller: Caller<T>, nodes_ptr: u32, nodes_len: u32) -> Result<u32>
where
    T: DistributedCtx<E>,
    E: Environment,
{
    let memory = get_memory(&mut caller)?;
    let node_ids = caller
        .data()
        .distributed()
        .map(|d| d.control.node_ids())
        .unwrap_or_else(|_| vec![]);
    let copy_nodes_len = node_ids.len().min(nodes_len as usize);
    memory
        .data_mut(&mut caller)
        .get_mut(
            nodes_ptr as usize..(nodes_ptr as usize + std::mem::size_of::<u64>() * copy_nodes_len),
        )
        .or_trap("lunatic::distributed::get_nodes::memory")?
        .copy_from_slice(unsafe { node_ids[..copy_nodes_len].align_to::<u8>().1 });
    Ok(copy_nodes_len as u32)
}

// Submits a lookup node query to the control server and waits for the results.
//
// Filtering is done based on tags which are `key=value` user defined node
// metadata, see CLI flag `tag`.
//
// Traps:
// * If the query is not a valid UTF-8 string
// * if any memory outside the guest heap space is referenced
fn exec_lookup_nodes<T, E>(
    mut caller: Caller<T>,
    query_ptr: u32,
    query_len: u32,
    query_id_ptr: u32,
    nodes_len_ptr: u32,
    error_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: DistributedCtx<E> + ErrorCtx + Send + 'static,
    E: Environment + 'static,
    for<'a> &'a T: Send,
{
    Box::new(async move {
        let memory = get_memory(&mut caller)?;
        let query_str = memory
            .data(&caller)
            .get(query_ptr as usize..(query_ptr + query_len) as usize)
            .or_trap("lunatic::distributed::lookup_nodes::query_ptr")?;
        let query = std::str::from_utf8(query_str)
            .or_trap("lunatic::distributed::lookup_nodes::query_str_utf8")?;
        let distributed = caller.data().distributed()?;
        match distributed.control.lookup_nodes(query).await {
            Ok((query_id, nodes_len)) => {
                memory
                    .write(&mut caller, query_id_ptr as usize, &query_id.to_le_bytes())
                    .or_trap("lunatic::distributed::lookup_nodes::query_id")?;
                memory
                    .write(
                        &mut caller,
                        nodes_len_ptr as usize,
                        &nodes_len.to_le_bytes(),
                    )
                    .or_trap("lunatic::distributed::lookup_nodes::nodes_len")?;
                Ok(0)
            }
            Err(error) => {
                let error_id = caller.data_mut().add_error_resource(error);
                memory
                    .write(&mut caller, error_ptr as usize, &error_id.to_le_bytes())
                    .or_trap("lunatic::distributed::lookup_nodes::error_ptr")?;
                Ok(1)
            }
        }
    })
}

// Copies node ids to guest memory from the lookup node query result, returns number of node ids copied.
//
// Traps:
// * If any memory outside the guest heap space is referenced.
fn copy_lookup_nodes_results<T, E>(
    mut caller: Caller<T>,
    query_id: u64,
    nodes_ptr: u32,
    nodes_len: u32,
    error_ptr: u32,
) -> Result<i32>
where
    T: DistributedCtx<E> + ErrorCtx,
    E: Environment,
{
    let memory = get_memory(&mut caller)?;
    if let Some(query_results) = caller
        .data()
        .distributed()
        .map(|d| d.control.query_result(&query_id))?
    {
        let nodes = query_results.1;
        let copy_nodes_len = nodes.len().min(nodes_len as usize);
        let memory = get_memory(&mut caller)?;
        memory
            .data_mut(&mut caller)
            .get_mut(
                nodes_ptr as usize
                    ..(nodes_ptr as usize + std::mem::size_of::<u64>() * copy_nodes_len),
            )
            .or_trap("lunatic::distributed::copy_lookup_nodes_results::memory")?
            .copy_from_slice(unsafe { nodes[..copy_nodes_len].align_to::<u8>().1 });
        Ok(copy_nodes_len as i32)
    } else {
        let error = anyhow!("Invalid query id");
        let error_id = caller.data_mut().add_error_resource(error);
        memory
            .write(&mut caller, error_ptr as usize, &error_id.to_le_bytes())
            .or_trap("lunatic::distributed::copy_lookup_nodes_results::error_ptr")?;
        Ok(-1)
    }
}

fn test_root_cert<T, E>(
    mut caller: Caller<T>,
    len_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: DistributedCtx<E> + Send,
    E: Environment,
{
    Box::new(async move {
        let memory = get_memory(&mut caller)?;
        let root_cert = lunatic_distributed::control::cert::test_root_cert()
            .or_trap("lunatic::distributed::test_root_cert")?;

        let cert_pem = root_cert.certificate_pem().to_owned();
        let key_pair_pem = root_cert.private_key_pem().to_owned();

        let data = bincode::serialize(&(cert_pem, key_pair_pem))
            .or_trap("lunatic::distributed::test_root_cert")?;
        let ptr = write_to_guest_vec(&mut caller, &memory, &data, len_ptr)
            .await
            .or_trap("lunatic::distributed::test_root_cert")?;

        Ok(ptr)
    })
}

fn default_server_certificates<T, E>(
    mut caller: Caller<T>,
    cert_pem_ptr: u32,
    cert_pem_len: u32,
    pk_pem_ptr: u32,
    pk_pem_len: u32,
    len_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: DistributedCtx<E> + Send,
    E: Environment,
{
    Box::new(async move {
        let memory = get_memory(&mut caller)?;

        let cert_pem_bytes = memory
            .data(&caller)
            .get(cert_pem_ptr as usize..(cert_pem_ptr + cert_pem_len) as usize)
            .or_trap("lunatic::distributed::spawn::default_server_certificates")?;
        let cert_pem = std::str::from_utf8(cert_pem_bytes)
            .or_trap("lunatic::distributed::default_server_certificates")?;

        let pk_pem_bytes = memory
            .data(&caller)
            .get(pk_pem_ptr as usize..(pk_pem_ptr + pk_pem_len) as usize)
            .or_trap("lunatic::distributed::default_server_certificates")?;
        let pk_pem = std::str::from_utf8(pk_pem_bytes)
            .or_trap("lunatic::distributed::default_server_certificates")?;

        let root_cert = CertificateAuthority::from_pem(cert_pem, pk_pem)
            .or_trap("lunatic::distributed::default_server_certificates")?;

        let (ctrl_cert, ctrl_pk) =
            lunatic_distributed::control::cert::default_server_certificates(&root_cert)?;

        let data = bincode::serialize(&(ctrl_cert, ctrl_pk))
            .or_trap("lunatic::distributed::default_server_certificates")?;
        let ptr = write_to_guest_vec(&mut caller, &memory, &data, len_ptr)
            .await
            .or_trap("lunatic::distributed::default_server_certificates")?;

        Ok(ptr)
    })
}

#[allow(clippy::too_many_arguments)]
fn sign_node<T, E>(
    mut caller: Caller<T>,
    cert_pem_ptr: u32,
    cert_pem_len: u32,
    pk_pem_ptr: u32,
    pk_pem_len: u32,
    csr_pem_ptr: u32,
    csr_pem_len: u32,
    len_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: DistributedCtx<E> + Send,
    E: Environment,
{
    Box::new(async move {
        let memory = get_memory(&mut caller)?;

        let cert_pem_bytes = memory
            .data(&caller)
            .get(cert_pem_ptr as usize..(cert_pem_ptr + cert_pem_len) as usize)
            .or_trap("lunatic::distributed::spawn::sign_node")?;
        let cert_pem =
            std::str::from_utf8(cert_pem_bytes).or_trap("lunatic::distributed::sign_node")?;

        let pk_pem_bytes = memory
            .data(&caller)
            .get(pk_pem_ptr as usize..(pk_pem_ptr + pk_pem_len) as usize)
            .or_trap("lunatic::distributed::sign_node")?;
        let pk_pem =
            std::str::from_utf8(pk_pem_bytes).or_trap("lunatic::distributed::sign_node")?;

        let csr_pem_bytes = memory
            .data(&caller)
            .get(csr_pem_ptr as usize..(csr_pem_ptr + csr_pem_len) as usize)
            .or_trap("lunatic::distributed::sign_node")?;
        let csr_pem =
            std::str::from_utf8(csr_pem_bytes).or_trap("lunatic::distributed::sign_node")?;

        let ca_cert = CertificateAuthority::from_pem(cert_pem, pk_pem)
            .or_trap("lunatic::distributed::sign_node")?;
        let mut csr = CertificateSigningRequestParams::from_pem(csr_pem)
            .or_trap("lunatic::distributed::sign_node")?;
        // Add json to custom certificate extension
        csr.params
            .custom_extensions
            .push(CustomExtension::from_oid_content(
                &SUBJECT_DIR_ATTRS,
                serde_json::to_string(&CertAttrs {
                    allowed_envs: vec![],
                    is_privileged: true,
                })
                .or_trap("lunatic::distributed::sign_node")?
                .to_der_vec()
                .or_trap("lunatic::distributed::sign_node")?,
            ));
        let cert_pem = csr
            .signed_by(ca_cert.issuer())
            .or_trap("lunatic::distributed::sign_node")?
            .pem();
        let data = bincode::serialize(&cert_pem).or_trap("lunatic::distributed::sign_node")?;
        let ptr = write_to_guest_vec(&mut caller, &memory, &data, len_ptr)
            .await
            .or_trap("lunatic::distributed::sign_node")?;

        Ok(ptr)
    })
}

// Similar to a local spawn, it spawns a new process using the passed in function inside a module
// as the entry point. The process is spawned on a node with id `node_id`.
//
// If `config_id` is -1, the calling process configuration is inherited. Any
// non-negative value selects that guest-visible configuration resource ID.
//
// The function arguments are passed as an array with the following structure:
// [0 byte = type ID; 1..17 bytes = value as u128, ...]
// The type ID follows the WebAssembly binary convention:
//  - 0x7F => i32
//  - 0x7E => i64
//  - 0x7B => v128
// If any other value is used as type ID, this function will trap. If your type
// would ordinarily occupy fewer than 16 bytes (e.g. in an i32 or i64), you MUST
// first convert it to an i128.
//
// Returns:
// * 0      on success - The ID of the newly created process is written to `id_ptr`
// * 1      If node does not exist
// * 2      If module does not exist
// * 3      If capability/config validation or the remote node rejects the request
// * 4      If the referenced process does not exist
// * 9027   If node connection error occurred
//
// Traps:
// * If the function string is not a valid utf8 string.
// * If the params array is in a wrong format.
// * If any memory outside the guest heap space is referenced.
#[allow(clippy::too_many_arguments)]
fn return_spawn_error<T: ErrorCtx>(
    caller: &mut Caller<T>,
    id_ptr: u32,
    code: u32,
    error: anyhow::Error,
) -> Result<u32> {
    let error_id = caller.data_mut().add_error_resource(error);
    let memory = get_memory(caller)?;
    memory
        .write(caller, id_ptr as usize, &error_id.to_le_bytes())
        .or_trap("lunatic::distributed::spawn::write_error_id")?;
    Ok(code)
}

#[allow(clippy::too_many_arguments)]
fn spawn<T, E>(
    mut caller: Caller<T>,
    node_id: u64,
    config_id: i64,
    module_id: u64,
    func_str_ptr: u32,
    func_str_len: u32,
    params_ptr: u32,
    params_len: u32,
    id_ptr: u32,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: DistributedCtx<E> + ResourceLimiter + Send + ErrorCtx + 'static,
    E: Environment,
    for<'a> &'a T: Send,
{
    Box::new(async move {
        let state = caller.data();
        let mut subject = AuditSubject::new()
            .with_environment_id(state.environment_id())
            .with_process_id(state.id());
        if let Ok(distributed) = state.distributed() {
            subject = subject.with_node_id(distributed.node_id());
        }
        let mut audit = PendingDistributedAudit::new(
            AuditEvent::DistributedRequestAuthorization,
            AuditAction::Spawn,
            subject,
            AuditTarget::new(AuditTargetKind::DistributedRequest)
                .with_node_id(node_id)
                .with_resource_id(module_id)
                .with_sensitive_data(SensitiveData::Redacted),
            AuditReason::InvalidInput,
        );
        if !caller.data().can_spawn() {
            audit.finish(AuditResult::Denied, AuditReason::CapabilityDenied);
            return return_spawn_error(
                &mut caller,
                id_ptr,
                3,
                anyhow!("Process doesn't have permissions to spawn sub-processes"),
            );
        }
        let memory = get_memory(&mut caller)?;
        let func_str = memory
            .data(&caller)
            .get(func_str_ptr as usize..(func_str_ptr + func_str_len) as usize)
            .or_trap("lunatic::distributed::spawn::func_str")?;

        let function =
            std::str::from_utf8(func_str).or_trap("lunatic::distributed::spawn::func_str_utf8")?;

        let params = memory
            .data(&caller)
            .get(params_ptr as usize..(params_ptr + params_len) as usize)
            .or_trap("lunatic::distributed::spawn::params")?;
        let params = params
            .chunks_exact(17)
            .map(|chunk| {
                let value = u128::from_le_bytes(chunk[1..].try_into()?);
                let result = match chunk[0] {
                    0x7F => Val::I32(value as i32),
                    0x7E => Val::I64(value as i64),
                    0x7B => Val::V128(value),
                    _ => return Err(anyhow!("Unsupported type ID")),
                };
                Ok(result)
            })
            .collect::<Result<Vec<_>>>()?;

        let state = caller.data();

        let config = match config_id {
            -1 => state.config().clone(),
            config_id => {
                let config = state
                    .config_resources()
                    .get(config_id as u64)
                    .or_trap("lunatic::distributed::spawn: Config ID doesn't exist")?
                    .clone();
                if let Err(reason) = state.config().validate_child_config(&config) {
                    audit.finish(AuditResult::Denied, AuditReason::DelegationExceedsParent);
                    return return_spawn_error(
                        &mut caller,
                        id_ptr,
                        3,
                        anyhow!("lunatic::distributed::spawn: delegated config denied: {reason}"),
                    );
                }
                Arc::new(config)
            }
        };
        if let Err(reason) = config.validate_distributed_config() {
            audit.finish(AuditResult::Denied, AuditReason::DelegationDenied);
            return return_spawn_error(
                &mut caller,
                id_ptr,
                3,
                anyhow!("lunatic::distributed::spawn: remote config denied: {reason}"),
            );
        }
        let config: Vec<u8> =
            rmp_serde::to_vec(config.as_ref()).map_err(|_| anyhow!("Error serializing config"))?;

        log::debug!(
            "Requesting spawn on node {node_id}, module {module_id}, with {} parameter(s)",
            params.len()
        );

        let self_node_id = state.distributed()?.node_id();
        let spawn_params = SpawnParams {
            env: EnvironmentId(state.environment_id()),
            src: ProcessId(state.id()),
            node: NodeId(node_id),
            spawn: Spawn {
                response_node_id: self_node_id,
                environment_id: state.environment_id(),
                function: function.to_string(),
                module_id,
                params,
                config,
            },
        };
        let node_client = state.distributed()?.node_client.clone();
        audit.mark_async();
        let message_id = match node_client.spawn(spawn_params).await {
            Ok(message_id) => message_id,
            Err(error) => {
                audit.finish(AuditResult::Failed, AuditReason::IoError);
                return Err(error);
            }
        };
        let spawn_response = match node_client.await_response(message_id).await {
            Ok(response) => response,
            Err(error) => {
                audit.finish(AuditResult::Failed, AuditReason::IoError);
                return Err(error);
            }
        };
        let (process_or_error_id, ret) = match spawn_response {
            distributed::message::ResponseContent::Spawned(process_id) => {
                audit.finish_with_target(
                    AuditResult::Succeeded,
                    AuditReason::Completed,
                    AuditTarget::new(AuditTargetKind::Process)
                        .with_node_id(node_id)
                        .with_environment_id(state.environment_id())
                        .with_process_id(process_id)
                        .with_sensitive_data(SensitiveData::Redacted),
                );
                Ok((process_id, 0))
            }
            distributed::message::ResponseContent::Error(error) => {
                let (code, message, reason): (u32, String, AuditReason) = match error {
                    ClientError::Unexpected(cause) => (3, cause, AuditReason::RuntimeFailure),
                    ClientError::Connection(cause) => (9027, cause, AuditReason::IoError),
                    ClientError::NodeNotFound => {
                        (1, "Node does not exist.".to_string(), AuditReason::NotFound)
                    }
                    ClientError::ModuleNotFound => (
                        2,
                        "Module does not exist.".to_string(),
                        AuditReason::NotFound,
                    ),
                    ClientError::ProcessNotFound => (
                        4,
                        "Process does not exist.".to_string(),
                        AuditReason::NotFound,
                    ),
                    ClientError::EnvironmentNotFound => (
                        4,
                        "Environment does not exist.".to_string(),
                        AuditReason::NotFound,
                    ),
                    ClientError::DeliveryBackpressure(cause) => {
                        (3, cause, AuditReason::ResourceLimit)
                    }
                    ClientError::DeliveryTooLarge(cause) | ClientError::DeliveryRejected(cause) => {
                        (3, cause, AuditReason::InvalidInput)
                    }
                    ClientError::ResponseTimeout => {
                        (9027, "Response timeout.".to_string(), AuditReason::TimedOut)
                    }
                };
                audit.finish(AuditResult::Failed, reason);
                Ok((caller.data_mut().add_error_resource(anyhow!(message)), code))
            }
            _ => Err(anyhow!("unreachable")),
        }?;

        memory
            .write(
                &mut caller,
                id_ptr as usize,
                &process_or_error_id.to_le_bytes(),
            )
            .or_trap("lunatic::distributed::spawn::write_id")?;

        Ok(ret)
    })
}

// Sends the message in scratch area to a process running on a node with id `node_id`.
//
// Success means that the destination accepted the message into its bounded
// signal/mailbox ingress. It does not mean that guest code has processed it.
//
// Returns:
// * 0      If message sent
// * 1      If process_id does not exist
// * 2      If node_id does not exist
// * 9027   If node connection error occurred
//
// Traps:
// * If it's called before creating the next message.
// * If the message contains resources
fn distributed_send_error_status(kind: SendErrorKind) -> u32 {
    match kind {
        SendErrorKind::NodeNotFound => 2,
        SendErrorKind::EnvironmentNotFound | SendErrorKind::ProcessNotFound => 1,
        SendErrorKind::Backpressure
        | SendErrorKind::MessageTooLarge
        | SendErrorKind::QueueClosed
        | SendErrorKind::Serialization
        | SendErrorKind::RemoteBackpressure
        | SendErrorKind::RemoteMessageTooLarge
        | SendErrorKind::RemoteRejected
        | SendErrorKind::Connection
        | SendErrorKind::ResponseTimeout
        | SendErrorKind::UnexpectedResponse => 9027,
    }
}

fn send<T, E>(
    mut caller: Caller<T>,
    node_id: u64,
    process_id: u64,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: DistributedCtx<E> + ProcessCtx<T> + Send + ErrorCtx + 'static,
    E: Environment,
    for<'a> &'a T: Send,
{
    Box::new(async move {
        let message = caller
            .data_mut()
            .message_scratch_area()
            .take()
            .or_trap("lunatic::distributed::send::no_message")?;

        let mut data_message = match message {
            Message::Data(data_message) => data_message,
            other => {
                caller.data_mut().message_scratch_area().replace(other);
                return Err(anyhow!("Only Message::Data can be sent across nodes."));
            }
        };
        if !data_message.resources.is_empty() {
            caller
                .data_mut()
                .message_scratch_area()
                .replace(Message::Data(data_message));
            return Err(anyhow!("Cannot send resources to remote nodes."));
        }

        let distributed_context = {
            let state = caller.data();
            state.distributed().map(|distributed| {
                (
                    EnvironmentId(state.environment_id()),
                    ProcessId(state.id()),
                    distributed.node_client.clone(),
                )
            })
        };
        let (env, src, node_client) = match distributed_context {
            Ok(context) => context,
            Err(error) => {
                caller
                    .data_mut()
                    .message_scratch_area()
                    .replace(Message::Data(data_message));
                return Err(error);
            }
        };
        let data = std::mem::take(&mut data_message.buffer);
        let tag = data_message.tag;
        let send_params = SendParams {
            source_env: env,
            target_env: env,
            src,
            node: NodeId(node_id),
            dest: ProcessId(process_id),
            tag,
            data,
        };
        match node_client.send(send_params).await {
            Ok(_) => Ok(0),
            Err(error) => {
                let status = distributed_send_error_status(error.kind());
                data_message.buffer = error.into_data();
                caller
                    .data_mut()
                    .message_scratch_area()
                    .replace(Message::Data(data_message));
                Ok(status)
            }
        }
    })
}

// Sends the message to a process on a node with id `node_id` and waits for a reply,
// but doesn't look through existing messages in the mailbox queue while waiting.
// This is an optimization that only makes sense with tagged messages.
// In a request/reply scenario we can tag the request message with an
// unique tag and just wait on it specifically.
//
// This operation needs to be an atomic host function, if we jumped back into the guest we could
// miss out on the incoming message before `receive` is called.
//
// If timeout is specified (value different from u64::MAX), the function will return on timeout
// expiration with value 9027.
//
// Returns:
// * 0    If message arrived.
// * 1    If process_id does not exist
// * 2    If node_id does not exist
// * 9027 If call timed out.
//
// Traps:
// * If it's called with wrong data in the scratch area.
// * If the message contains resources
fn send_receive_skip_search<T, E>(
    mut caller: Caller<T>,
    node_id: u64,
    process_id: u64,
    wait_on_tag: i64,
    timeout_duration: u64,
) -> Box<dyn Future<Output = Result<u32>> + Send + '_>
where
    T: DistributedCtx<E> + ProcessCtx<T> + Send + 'static,
    E: Environment,
    for<'a> &'a T: Send,
{
    Box::new(async move {
        let message = caller
            .data_mut()
            .message_scratch_area()
            .take()
            .or_trap("lunatic::distributed::send_receive_skip_search")?;

        let mut data_message = match message {
            Message::Data(data_message) => data_message,
            other => {
                caller.data_mut().message_scratch_area().replace(other);
                return Err(anyhow!("Only Message::Data can be sent across nodes."));
            }
        };
        if !data_message.resources.is_empty() {
            caller
                .data_mut()
                .message_scratch_area()
                .replace(Message::Data(data_message));
            return Err(anyhow!("Cannot send resources to remote nodes."));
        }

        let distributed_context = {
            let state = caller.data();
            state.distributed().map(|distributed| {
                (
                    EnvironmentId(state.environment_id()),
                    ProcessId(state.id()),
                    distributed.node_client.clone(),
                )
            })
        };
        let (env, src, node_client) = match distributed_context {
            Ok(context) => context,
            Err(error) => {
                caller
                    .data_mut()
                    .message_scratch_area()
                    .replace(Message::Data(data_message));
                return Err(error);
            }
        };
        let data = std::mem::take(&mut data_message.buffer);
        let tag = data_message.tag;
        let send_params = SendParams {
            source_env: env,
            target_env: env,
            src,
            node: NodeId(node_id),
            dest: ProcessId(process_id),
            tag,
            data,
        };
        let wait_started = Instant::now();
        let send_result = if timeout_duration == u64::MAX {
            node_client.send(send_params).await
        } else {
            node_client
                .send_with_timeout(send_params, Duration::from_millis(timeout_duration))
                .await
        };
        if let Err(error) = send_result {
            let status = distributed_send_error_status(error.kind());
            data_message.buffer = error.into_data();
            caller
                .data_mut()
                .message_scratch_area()
                .replace(Message::Data(data_message));
            return Ok(status);
        }

        let tags = [wait_on_tag];
        let pop_skip_search = caller.data_mut().mailbox().pop_skip_search(Some(&tags));
        if let Ok(message) = match timeout_duration {
            // Without timeout
            u64::MAX => Ok(pop_skip_search.await),
            // Delivery acknowledgement and reply share one caller budget.
            t => {
                timeout(
                    Duration::from_millis(t).saturating_sub(wait_started.elapsed()),
                    pop_skip_search,
                )
                .await
            }
        } {
            // Put the message into the scratch area
            caller.data_mut().message_scratch_area().replace(message);
            Ok(0)
        } else {
            Ok(9027)
        }
    })
}

// Returns the id of the node that the current process is running on
fn node_id<T, E>(caller: Caller<T>) -> u64
where
    T: DistributedCtx<E>,
    E: Environment,
{
    caller
        .data()
        .distributed()
        .as_ref()
        .map(|d| d.node_id())
        .unwrap_or(0)
}

// Returns id of the module that the current process is spawned from
fn module_id<T, E>(caller: Caller<T>) -> u64
where
    T: DistributedCtx<E>,
    E: Environment,
{
    caller.data().module_id()
}

#[cfg(test)]
mod tests {
    use super::distributed_send_error_status;
    use lunatic_distributed::distributed::client::SendErrorKind;

    #[test]
    fn delivery_error_status_preserves_the_existing_guest_abi() {
        assert_eq!(
            distributed_send_error_status(SendErrorKind::EnvironmentNotFound),
            1
        );
        assert_eq!(
            distributed_send_error_status(SendErrorKind::ProcessNotFound),
            1
        );
        assert_eq!(
            distributed_send_error_status(SendErrorKind::NodeNotFound),
            2
        );
        for kind in [
            SendErrorKind::RemoteBackpressure,
            SendErrorKind::RemoteMessageTooLarge,
            SendErrorKind::RemoteRejected,
            SendErrorKind::Connection,
            SendErrorKind::ResponseTimeout,
            SendErrorKind::UnexpectedResponse,
        ] {
            assert_eq!(distributed_send_error_status(kind), 9027);
        }
    }
}
