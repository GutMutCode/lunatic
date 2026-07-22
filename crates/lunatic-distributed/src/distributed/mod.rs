pub mod client;
pub mod global_process_id;
pub mod message;
pub mod registry;
pub mod registry_coordination;
pub mod server;

use lunatic_common_api::{
    emit_audit_event, AuditAction, AuditEvent, AuditEventV1, AuditReason, AuditResult,
    AuditSubject, AuditTarget, AuditTargetKind, SensitiveData,
};

use crate::quic::VerifiedNodeId;

pub use client::{
    Client, DistributedLimits, OutboundLimitKind, OutboundLimits, OutboundSaturationError,
    RegistryProcessRegistration,
};
pub use global_process_id::{GlobalProcessId, MAX_NODE_ID};
pub use registry::{
    DistributedRegistry, ProcessName, RegistrationScope, RegistryCapacityError, RegistryEntry,
    RegistryLimits, RegistryUsage,
};
pub use registry_coordination::{
    GlobalRegisterResult, GlobalUnregisterResult, RegistryCoordinationMessage, RegistryCoordinator,
};

pub(crate) fn audit_verified_peer_protocol_denial(source_node_id: VerifiedNodeId) {
    emit_audit_event(verified_peer_protocol_denial_event(source_node_id));
}

pub(crate) fn verified_peer_protocol_denial_event(source_node_id: VerifiedNodeId) -> AuditEventV1 {
    AuditEventV1::new(
        AuditEvent::DistributedRequestAuthorization,
        AuditAction::Validate,
        AuditResult::Denied,
        AuditReason::ProtocolDenied,
        AuditSubject::new().with_node_id(source_node_id.get()),
        AuditTarget::new(AuditTargetKind::DistributedRequest)
            .with_sensitive_data(SensitiveData::Redacted),
    )
}
