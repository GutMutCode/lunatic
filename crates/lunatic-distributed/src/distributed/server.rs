use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use lunatic_common_api::{
    emit_audit_event, AuditAction, AuditEvent, AuditEventV1, AuditReason, AuditResult,
    AuditSubject, AuditTarget, AuditTargetKind, SensitiveData,
};

use lunatic_process::{
    config::ProcessConfig,
    env::{Environment, Environments, ProcessLimitReached},
    message::{DataMessage, Message},
    runtimes::{wasmtime::WasmtimeRuntime, Modules, RawWasm},
    state::{ProcessState, SignalSendErrorKind},
    Signal,
};
use rcgen::{CertificateParams, DnType};
use wasmtime::ResourceLimiter;

use crate::{
    control::cert::CertificateRequest,
    distributed::audit_verified_peer_protocol_denial,
    distributed::message::{Request, Response},
    quic::{self, NodeEnvPermission, VerifiedNodeId},
    DistributedCtx, DistributedProcessState,
};

use super::{
    client::{Client, NodeId, ResponseParams},
    message::{ClientError, ResponseContent, Spawn},
};

pub struct ServerCtx<T, E: Environment> {
    pub envs: Arc<dyn Environments<Env = E>>,
    pub modules: Modules<T>,
    pub distributed: DistributedProcessState,
    pub runtime: WasmtimeRuntime,
    pub node_client: Client,
    pub allowed_envs: Option<HashSet<u64>>,
}

struct PendingServerAudit {
    event: Option<AuditEvent>,
    action: AuditAction,
    subject: AuditSubject,
    target: AuditTarget,
    fallback_result: AuditResult,
    fallback_reason: AuditReason,
}

impl PendingServerAudit {
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

    fn mark_failed(&mut self, reason: AuditReason) {
        self.fallback_result = AuditResult::Failed;
        self.fallback_reason = reason;
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

impl Drop for PendingServerAudit {
    fn drop(&mut self) {
        if self.event.is_some() {
            let reason = self.fallback_reason;
            self.emit(self.fallback_result, reason, None);
        }
    }
}

fn audit_request_authorization(
    verified_remote_node_id: u64,
    environment_id: u64,
    result: AuditResult,
    reason: AuditReason,
) {
    emit_audit_event(request_authorization_event(
        verified_remote_node_id,
        environment_id,
        result,
        reason,
    ));
}

fn request_authorization_event(
    verified_remote_node_id: u64,
    environment_id: u64,
    result: AuditResult,
    reason: AuditReason,
) -> AuditEventV1 {
    AuditEventV1::new(
        AuditEvent::DistributedRequestAuthorization,
        AuditAction::Validate,
        result,
        reason,
        AuditSubject::new().with_node_id(verified_remote_node_id),
        AuditTarget::new(AuditTargetKind::DistributedRequest)
            .with_environment_id(environment_id)
            .with_sensitive_data(SensitiveData::Redacted),
    )
}

fn classify_spawn_error(error: &anyhow::Error) -> (AuditResult, AuditReason) {
    if error.downcast_ref::<ProcessLimitReached>().is_some() {
        (AuditResult::Denied, AuditReason::ResourceLimit)
    } else {
        (AuditResult::Failed, AuditReason::RuntimeFailure)
    }
}

impl<T: 'static, E: Environment> Clone for ServerCtx<T, E> {
    fn clone(&self) -> Self {
        Self {
            envs: self.envs.clone(),
            modules: self.modules.clone(),
            distributed: self.distributed.clone(),
            runtime: self.runtime.clone(),
            node_client: self.node_client.clone(),
            allowed_envs: self.allowed_envs.clone(),
        }
    }
}

pub fn test_root_cert() -> String {
    crate::control::cert::TEST_ROOT_CERT.to_string()
}

pub fn root_cert(ca_cert: &str) -> Result<String> {
    let cert = std::fs::read(ca_cert)?;
    Ok(std::str::from_utf8(&cert)?.to_string())
}

pub fn gen_node_cert(node_name: &str) -> Result<CertificateRequest> {
    let mut params = CertificateParams::new(vec![node_name.to_string()])
        .map_err(|error| anyhow!("Error while generating node certificate parameters: {error}"))?;
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Lunatic Inc.");
    params.distinguished_name.push(DnType::CommonName, "Node");
    CertificateRequest::new(params)
        .map_err(|error| anyhow!("Error while generating node certificate: {error}"))
}

pub async fn node_server<T, E>(
    ctx: ServerCtx<T, E>,
    socket: SocketAddr,
    ca_cert: String,
    certs: Vec<String>,
    key: String,
) -> Result<()>
where
    T: ProcessState + ResourceLimiter + DistributedCtx<E> + Send + Sync + 'static,
    E: Environment + 'static,
{
    let mut quic_server = quic::new_quic_server(socket, certs, &key, &ca_cert)?;
    if let Err(e) = quic::handle_node_server(&mut quic_server, ctx.clone()).await {
        log::error!("Node server stopped {e}")
    };
    Ok(())
}

pub(crate) async fn handle_message<T, E>(
    ctx: ServerCtx<T, E>,
    msg_id: u64,
    msg: Request,
    peer_node_id: VerifiedNodeId,
    node_permissions: Arc<NodeEnvPermission>,
) where
    T: ProcessState
        + DistributedCtx<E>
        + ResourceLimiter
        + Send
        + Sync
        + lunatic_process::reloadable_state::ReloadableState
        + 'static,
    E: Environment + 'static,
{
    if let Err(e) = handle_message_err(ctx, msg_id, msg, peer_node_id, node_permissions).await {
        log::error!("Error handling message: {e}");
    }
}

async fn handle_message_err<T, E>(
    ctx: ServerCtx<T, E>,
    msg_id: u64,
    msg: Request,
    peer_node_id: VerifiedNodeId,
    node_permissions: Arc<NodeEnvPermission>,
) -> Result<()>
where
    T: ProcessState
        + DistributedCtx<E>
        + ResourceLimiter
        + Send
        + Sync
        + lunatic_process::reloadable_state::ReloadableState
        + 'static,
    E: Environment + 'static,
{
    if !ctx.node_client.is_active_node(peer_node_id.get()) {
        audit_verified_peer_protocol_denial(peer_node_id);
        return Err(anyhow!(
            "Authenticated peer is no longer an active topology member"
        ));
    }

    let claimed_node_id = match &msg {
        Request::Spawn(spawn) => Some(spawn.response_node_id),
        Request::Message { node_id, .. } | Request::Registry { node_id, .. } => Some(*node_id),
        Request::Response(_) => None,
    };
    if claimed_node_id.is_some_and(|claimed| claimed != peer_node_id.get()) {
        audit_verified_peer_protocol_denial(peer_node_id);
        return Err(anyhow!(
            "Distributed request source did not match authenticated peer"
        ));
    }

    let env_id = match &msg {
        Request::Spawn(spawn) => Some(spawn.environment_id),
        Request::Message {
            environment_id,
            process_id: _,
            tag: _,
            data: _,
            ..
        } => Some(*environment_id),
        Request::Response(_) => None,
        Request::Registry { .. } => None,
    };
    if let Some(env_id) = env_id {
        if let Some(ref allowed_envs) = node_permissions.0 {
            if !allowed_envs.contains(&env_id) {
                audit_request_authorization(
                    peer_node_id.get(),
                    env_id,
                    AuditResult::Denied,
                    AuditReason::PolicyDenied,
                );
                ctx.node_client
                    .send_response(ResponseParams {
                        node_id: NodeId(peer_node_id.get()),
                        response: Response {
                            message_id: msg_id,
                            content: ResponseContent::Error(ClientError::Unexpected(format!(
                    "The node sending the request does not have access to the environment {env_id}"
                ))),
                        },
                    })
                    .await?;
                return Ok(());
            }
        }
        if let Some(ref allowed_envs) = ctx.allowed_envs {
            if !allowed_envs.contains(&env_id) {
                audit_request_authorization(
                    peer_node_id.get(),
                    env_id,
                    AuditResult::Denied,
                    AuditReason::PolicyDenied,
                );
                ctx.node_client
                    .send_response(ResponseParams {
                        node_id: NodeId(peer_node_id.get()),
                        response: Response {
                            message_id: msg_id,
                            content: ResponseContent::Error(ClientError::Unexpected(format!(
                                "This node does not have access to environment {env_id}"
                            ))),
                        },
                    })
                    .await?;
                return Ok(());
            }
        }
        audit_request_authorization(
            peer_node_id.get(),
            env_id,
            AuditResult::Allowed,
            AuditReason::PolicyAllowed,
        );
    }
    match msg {
        Request::Spawn(spawn) => {
            log::trace!("lunatic::distributed::server process Spawn");
            match handle_spawn(ctx.clone(), spawn).await {
                Ok(Ok(id)) => {
                    log::trace!("lunatic::distributed::server Spawned {id}");
                    ctx.node_client
                        .send_response(ResponseParams {
                            node_id: NodeId(peer_node_id.get()),
                            response: Response {
                                message_id: msg_id,
                                content: ResponseContent::Spawned(id),
                            },
                        })
                        .await?;
                }
                Ok(Err(client_error)) => {
                    log::trace!("lunatic::distributed::server Spawn error: {client_error:?}");
                    ctx.node_client
                        .send_response(ResponseParams {
                            node_id: NodeId(peer_node_id.get()),
                            response: Response {
                                message_id: msg_id,
                                content: ResponseContent::Error(client_error),
                            },
                        })
                        .await?;
                }
                Err(error) => {
                    log::trace!("lunatic::distributed::server Spawn error: {error}");
                    ctx.node_client
                        .send_response(ResponseParams {
                            node_id: NodeId(peer_node_id.get()),
                            response: Response {
                                message_id: msg_id,
                                content: ResponseContent::Error(ClientError::Unexpected(
                                    error.to_string(),
                                )),
                            },
                        })
                        .await?;
                }
            };
        }
        Request::Message {
            node_id: _,
            environment_id,
            process_id,
            tag,
            data,
        } => {
            log::trace!("distributed::server process Message");
            match handle_process_message(ctx.clone(), environment_id, process_id, tag, data).await {
                Ok(_) => {
                    ctx.node_client
                        .send_response(ResponseParams {
                            node_id: NodeId(peer_node_id.get()),
                            response: Response {
                                message_id: msg_id,
                                content: ResponseContent::Sent,
                            },
                        })
                        .await?;
                }
                Err(error) => {
                    ctx.node_client
                        .send_response(ResponseParams {
                            node_id: NodeId(peer_node_id.get()),
                            response: Response {
                                message_id: msg_id,
                                content: ResponseContent::Error(error),
                            },
                        })
                        .await?;
                }
            }
        }
        Request::Response(response) => {
            log::trace!("distributed::server process Response");
            ctx.node_client
                .recv_response(peer_node_id, response)
                .await?;
        }
        Request::Registry { node_id, message } => {
            log::trace!("distributed::server process Registry");
            ctx.node_client
                .handle_registry_message(peer_node_id, node_id, message)
                .await?;
        }
    };
    Ok(())
}

fn decode_distributed_config<C: ProcessConfig>(encoded: &[u8], _environment_id: u64) -> Result<C> {
    let config: C = rmp_serde::from_slice(encoded)?;
    // Each config implementation applies its receiver-side policy here, including numeric
    // ceilings and rejection of receiver-local capabilities such as host filesystem paths.
    if let Err(reason) = config.validate_distributed_config() {
        return Err(anyhow!(
            "distributed spawn config denied by receiver: {reason}"
        ));
    }
    Ok(config)
}

async fn handle_spawn<T, E>(ctx: ServerCtx<T, E>, spawn: Spawn) -> Result<Result<u64, ClientError>>
where
    T: ProcessState
        + DistributedCtx<E>
        + ResourceLimiter
        + Send
        + Sync
        + lunatic_process::reloadable_state::ReloadableState
        + 'static,
    E: Environment + 'static,
{
    let Spawn {
        environment_id,
        module_id,
        function,
        params,
        config,
        ..
    } = spawn;
    let mut audit = PendingServerAudit::new(
        AuditEvent::ProcessSpawn,
        AuditAction::Spawn,
        AuditSubject::new()
            .with_node_id(ctx.distributed.node_id())
            .with_environment_id(environment_id),
        AuditTarget::new(AuditTargetKind::Module)
            .with_resource_id(module_id)
            .with_sensitive_data(SensitiveData::Redacted),
        AuditReason::RuntimeFailure,
    );
    let config: T::Config = match decode_distributed_config(&config, environment_id) {
        Ok(config) => config,
        Err(error) => {
            audit.finish(AuditResult::Denied, AuditReason::DelegationDenied);
            return Err(error);
        }
    };
    let config = Arc::new(config);

    let module = match ctx.modules.get(module_id) {
        Some(module) => module,
        None => {
            audit.mark_async();
            let module_bytes = ctx
                .distributed
                .control
                .get_module(module_id, environment_id)
                .await;
            audit.mark_failed(AuditReason::RuntimeFailure);
            if let Ok(bytes) = module_bytes {
                let wasm = RawWasm::new(Some(module_id), bytes);
                audit.mark_async();
                let compiled = ctx.modules.compile(ctx.runtime.clone(), wasm).await;
                audit.mark_failed(AuditReason::RuntimeFailure);
                compiled??
            } else {
                audit.finish(AuditResult::Failed, AuditReason::NotFound);
                return Ok(Err(ClientError::ModuleNotFound));
            }
        }
    };

    audit.mark_async();
    let env = ctx.envs.get(environment_id).await;
    audit.mark_failed(AuditReason::RuntimeFailure);

    let env = match env {
        Some(env) => env,
        None => {
            audit.mark_async();
            let created = ctx.envs.create(environment_id).await;
            audit.mark_failed(AuditReason::RuntimeFailure);
            created?
        }
    };

    audit.mark_async();
    let can_spawn = env.can_spawn_next_process().await;
    audit.mark_failed(AuditReason::RuntimeFailure);
    if let Err(error) = can_spawn {
        audit.finish(AuditResult::Denied, AuditReason::ResourceLimit);
        return Err(error);
    }

    let distributed = ctx.distributed.clone();
    let runtime = ctx.runtime.clone();
    let state = match T::new_dist_state(env.clone(), distributed, runtime, module.clone(), config) {
        Ok(state) => state,
        Err(error) => {
            audit.finish(AuditResult::Failed, AuditReason::RuntimeFailure);
            return Err(error);
        }
    };
    let params: Vec<wasmtime::Val> = params.into_iter().map(Into::into).collect();
    audit.mark_async();
    let (_handle, proc) = match lunatic_process::wasm::spawn_wasm(
        env,
        ctx.runtime,
        &module,
        state,
        &function,
        params,
        None,
    )
    .await
    {
        Ok(spawned) => spawned,
        Err(error) => {
            let (result, reason) = classify_spawn_error(&error);
            audit.finish(result, reason);
            return Err(error);
        }
    };
    audit.finish_with_target(
        AuditResult::Succeeded,
        AuditReason::Completed,
        AuditTarget::new(AuditTargetKind::Process)
            .with_node_id(ctx.distributed.node_id())
            .with_environment_id(environment_id)
            .with_process_id(proc.id())
            .with_sensitive_data(SensitiveData::Redacted),
    );
    Ok(Ok(proc.id()))
}

async fn handle_process_message<T, E>(
    ctx: ServerCtx<T, E>,
    environment_id: u64,
    process_id: u64,
    tag: Option<i64>,
    data: Vec<u8>,
) -> std::result::Result<(), ClientError>
where
    T: ProcessState
        + DistributedCtx<E>
        + ResourceLimiter
        + Send
        + lunatic_process::reloadable_state::ReloadableState
        + 'static,
    E: Environment,
{
    deliver_process_message(ctx.envs.as_ref(), environment_id, process_id, tag, data).await
}

async fn deliver_process_message<E: Environment>(
    envs: &dyn Environments<Env = E>,
    environment_id: u64,
    process_id: u64,
    tag: Option<i64>,
    data: Vec<u8>,
) -> std::result::Result<(), ClientError> {
    let env = envs.get(environment_id).await;
    if let Some(env) = env {
        if let Some(proc) = env.get_process(process_id) {
            proc.send(Signal::Message(Message::Data(DataMessage::new_from_vec(
                tag, data,
            ))))
            .map_err(|error| match error.kind() {
                SignalSendErrorKind::Closed => ClientError::ProcessNotFound,
                SignalSendErrorKind::MailboxFull | SignalSendErrorKind::QueueFull => {
                    ClientError::DeliveryBackpressure(error.to_string())
                }
                SignalSendErrorKind::MessageTooLarge => {
                    ClientError::DeliveryTooLarge(error.to_string())
                }
                SignalSendErrorKind::TooManyMessageResources => {
                    ClientError::DeliveryRejected(error.to_string())
                }
            })?;
        } else {
            return Err(ClientError::ProcessNotFound);
        }
        Ok(())
    } else {
        Err(ClientError::EnvironmentNotFound)
    }
}

#[cfg(test)]
mod tests {
    use lunatic_common_api::{AuditReason, AuditResult};
    use lunatic_process::{
        config::ProcessConfig,
        env::{Environment, Environments, LunaticEnvironments, ProcessLimitReached},
        state::SignalSendError,
        Process, Signal,
    };
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    use super::{
        classify_spawn_error, decode_distributed_config, deliver_process_message,
        request_authorization_event, ClientError,
    };

    #[derive(Clone, Copy)]
    enum DeliveryBehavior {
        Accept,
        Closed,
        Backpressure,
        TooLarge,
        Rejected,
    }

    struct DeliveryProcess {
        id: u64,
        behavior: DeliveryBehavior,
    }

    impl Process for DeliveryProcess {
        fn id(&self) -> u64 {
            self.id
        }

        fn send(&self, signal: Signal) -> std::result::Result<(), SignalSendError> {
            match self.behavior {
                DeliveryBehavior::Accept => Ok(()),
                DeliveryBehavior::Closed => Err(SignalSendError::Closed(signal)),
                DeliveryBehavior::Backpressure => Err(SignalSendError::MailboxFull(signal)),
                DeliveryBehavior::TooLarge => Err(SignalSendError::MessageTooLarge {
                    signal,
                    actual: 2,
                    max: 1,
                }),
                DeliveryBehavior::Rejected => Err(SignalSendError::TooManyMessageResources {
                    signal,
                    actual: 2,
                    max: 1,
                }),
            }
        }
    }

    fn delivery_process(id: u64, behavior: DeliveryBehavior) -> Arc<dyn Process> {
        Arc::new(DeliveryProcess { id, behavior })
    }

    #[test]
    fn atomic_process_admission_failure_is_a_resource_denial() {
        let error = anyhow::Error::new(ProcessLimitReached::new(7, 0));
        assert_eq!(
            classify_spawn_error(&error),
            (AuditResult::Denied, AuditReason::ResourceLimit)
        );
        assert_eq!(
            classify_spawn_error(&anyhow::anyhow!("runtime failure")),
            (AuditResult::Failed, AuditReason::RuntimeFailure)
        );
    }

    #[tokio::test]
    async fn delivery_distinguishes_missing_environment_and_process() {
        let environments = LunaticEnvironments::default();
        assert_eq!(
            deliver_process_message(&environments, 7, 11, None, vec![]).await,
            Err(ClientError::EnvironmentNotFound)
        );

        environments.create(7).await.unwrap();
        assert_eq!(
            deliver_process_message(&environments, 7, 11, None, vec![]).await,
            Err(ClientError::ProcessNotFound)
        );
    }

    #[tokio::test]
    async fn delivery_maps_receiver_admission_failures_to_wire_errors() {
        let environments = LunaticEnvironments::default();
        let environment = environments.create(7).await.unwrap();
        for (process_id, behavior) in [
            (1, DeliveryBehavior::Accept),
            (2, DeliveryBehavior::Closed),
            (3, DeliveryBehavior::Backpressure),
            (4, DeliveryBehavior::TooLarge),
            (5, DeliveryBehavior::Rejected),
        ] {
            environment
                .add_process(process_id, delivery_process(process_id, behavior))
                .unwrap();
        }

        assert_eq!(
            deliver_process_message(&environments, 7, 1, Some(9), vec![1]).await,
            Ok(())
        );
        assert_eq!(
            deliver_process_message(&environments, 7, 2, None, vec![]).await,
            Err(ClientError::ProcessNotFound)
        );
        assert!(matches!(
            deliver_process_message(&environments, 7, 3, None, vec![]).await,
            Err(ClientError::DeliveryBackpressure(_))
        ));
        assert!(matches!(
            deliver_process_message(&environments, 7, 4, None, vec![]).await,
            Err(ClientError::DeliveryTooLarge(_))
        ));
        assert!(matches!(
            deliver_process_message(&environments, 7, 5, None, vec![]).await,
            Err(ClientError::DeliveryRejected(_))
        ));
    }

    #[test]
    fn receiver_authorization_event_uses_verified_remote_identity() {
        let event =
            request_authorization_event(7, 13, AuditResult::Allowed, AuditReason::PolicyAllowed);
        assert_eq!(event.result(), AuditResult::Allowed);
        assert_eq!(event.reason(), AuditReason::PolicyAllowed);
        assert_eq!(event.subject().node_id(), Some(7));
        assert_eq!(event.subject().environment_id(), None);
        assert_eq!(event.subject().process_id(), None);
        assert_eq!(event.target().node_id(), None);
        assert_eq!(event.target().environment_id(), Some(13));
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct ReceiverTestConfig {
        max_fuel: Option<u64>,
        max_memory: usize,
        has_host_local_authority: bool,
    }

    impl ProcessConfig for ReceiverTestConfig {
        fn set_max_fuel(&mut self, max_fuel: Option<u64>) {
            self.max_fuel = max_fuel;
        }

        fn get_max_fuel(&self) -> Option<u64> {
            self.max_fuel
        }

        fn set_max_memory(&mut self, max_memory: usize) {
            self.max_memory = max_memory;
        }

        fn get_max_memory(&self) -> usize {
            self.max_memory
        }

        fn validate_distributed_config(&self) -> Result<(), String> {
            if self.has_host_local_authority {
                Err("test host-local authority".into())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn receiver_decodes_allowed_config_at_production_boundary() {
        let encoded = rmp_serde::to_vec(&ReceiverTestConfig::default()).unwrap();
        let decoded: ReceiverTestConfig = decode_distributed_config(&encoded, 7).unwrap();
        assert!(!decoded.has_host_local_authority);
    }

    #[test]
    fn receiver_rejects_host_local_authority_after_deserialization() {
        let encoded = rmp_serde::to_vec(&ReceiverTestConfig {
            has_host_local_authority: true,
            ..Default::default()
        })
        .unwrap();
        let error = decode_distributed_config::<ReceiverTestConfig>(&encoded, 7).unwrap_err();
        assert!(error.to_string().contains("denied by receiver"));
        assert!(error.to_string().contains("host-local"));
    }
}
