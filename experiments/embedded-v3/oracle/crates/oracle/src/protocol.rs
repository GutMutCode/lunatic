use std::fmt;
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

pub const SCHEMA_VERSION: u32 = 3;
pub const MAX_WIRE_STRING_BYTES: usize = 4_096;
pub const MAX_BUILD_ID_BYTES: usize = 128;
pub const MAX_LOGICAL_VERSION_BYTES: usize = 128;
pub const MAX_ARTIFACT_REF_BYTES: usize = 512;
pub const MAX_ROLLOUT_TARGETS: usize = 32;
pub const COMPARISON_TENANT_COUNT: u32 = 32;
pub const MAX_RUN_ID_BYTES: usize = 128;
pub const MAX_ROLLOUT_ID_BYTES: usize = 128;
pub const MAX_COMMAND_PAYLOAD_BYTES: usize = 1_025;
pub const MAX_AUTHORITY_ARTIFACT_BYTES: usize = 65_536;
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// A wire value that can only deserialize from the frozen schema version.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SchemaVersion;

impl Serialize for SchemaVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(SCHEMA_VERSION)
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionVisitor;

        impl Visitor<'_> for VersionVisitor {
            type Value = SchemaVersion;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "the integer schema version {SCHEMA_VERSION}")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == u64::from(SCHEMA_VERSION) {
                    Ok(SchemaVersion)
                } else {
                    Err(E::custom(format_args!(
                        "unsupported schema_version {value}; expected {SCHEMA_VERSION}"
                    )))
                }
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == i64::from(SCHEMA_VERSION) {
                    Ok(SchemaVersion)
                } else {
                    Err(E::custom(format_args!(
                        "unsupported schema_version {value}; expected {SCHEMA_VERSION}"
                    )))
                }
            }
        }

        deserializer.deserialize_u32(VersionVisitor)
    }
}

macro_rules! decimal_u64_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u64);
        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0.to_string())
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                if value.is_empty()
                    || (value.len() > 1 && value.starts_with('0'))
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err(de::Error::custom(concat!(
                        stringify!($name),
                        " must be a canonical decimal u64 string"
                    )));
                }
                value
                    .parse::<u64>()
                    .map(Self)
                    .map_err(|_| de::Error::custom(concat!(stringify!($name), " exceeds u64")))
            }
        }
    };
}

macro_rules! numeric_id {
    ($name:ident, $inner:ty) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub $inner);

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

decimal_u64_id!(RequestId);
decimal_u64_id!(EventSeq);
numeric_id!(TenantId, u32);
decimal_u64_id!(CommandId);
decimal_u64_id!(DecimalU64);

/// Oracle-owned tenant generation, encoded as a canonical decimal JSON string.
/// Candidates echo it; they never choose it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IncarnationToken(u64);

impl IncarnationToken {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for IncarnationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl Serialize for IncarnationToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for IncarnationToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.is_empty()
            || (value.len() > 1 && value.starts_with('0'))
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(de::Error::custom(
                "incarnation must be a canonical decimal u64 string",
            ));
        }
        value
            .parse::<u64>()
            .map(Self)
            .map_err(|_| de::Error::custom("incarnation exceeds u64"))
    }
}

/// The three decision-bearing production candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateKind {
    Lunatic,
    Extism,
    RawWasmtime,
}

impl fmt::Display for CandidateKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Lunatic => "lunatic",
            Self::Extism => "extism",
            Self::RawWasmtime => "raw-wasmtime",
        })
    }
}

impl FromStr for CandidateKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "lunatic" => Ok(Self::Lunatic),
            "extism" => Ok(Self::Extism),
            "raw-wasmtime" | "raw_wasmtime" => Ok(Self::RawWasmtime),
            other => Err(format!(
                "unknown candidate {other:?}; expected lunatic, extism, or raw-wasmtime"
            )),
        }
    }
}

/// Test-double identity is intentionally outside the production candidate set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestDoubleKind {
    Fake,
}

/// Identity exchanged only during hello. Its untagged representation preserves
/// the compact production strings while preventing a fake from entering
/// decision-bearing candidate collections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CandidateIdentity {
    Production(CandidateKind),
    TestDouble(TestDoubleKind),
}

impl CandidateIdentity {
    pub const fn fake_test_double() -> Self {
        Self::TestDouble(TestDoubleKind::Fake)
    }
}

impl From<CandidateKind> for CandidateIdentity {
    fn from(value: CandidateKind) -> Self {
        Self::Production(value)
    }
}

impl fmt::Display for CandidateIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Production(candidate) => candidate.fmt(formatter),
            Self::TestDouble(TestDoubleKind::Fake) => formatter.write_str("fake"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlEnvelope {
    pub schema_version: SchemaVersion,
    pub request_id: RequestId,
    pub message: ControlMessage,
}

impl ControlEnvelope {
    pub fn new(request_id: impl Into<RequestId>, message: ControlMessage) -> Self {
        Self {
            schema_version: SchemaVersion,
            request_id: request_id.into(),
            message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlMessage {
    Hello(HelloControl),
    Init(InitControl),
    CreateTenant(CreateTenantControl),
    Command(CommandControl),
    InjectFault(InjectFaultControl),
    Rollout(RolloutControl),
    Snapshot(SnapshotControl),
    SetDequeueGate(SetDequeueGateControl),
    AuthorityProbe(AuthorityProbeControl),
    Quiesce(QuiesceControl),
    TeardownTenant(TeardownTenantControl),
    Shutdown(ShutdownControl),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloControl {
    pub oracle_name: String,
    pub expected_candidate: CandidateIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitControl {
    pub run_id: String,
    pub tenant_capacity: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTenantControl {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub logical_version: String,
    pub build_id: String,
    pub artifact_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandControl {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub command_id: CommandId,
    pub operation: CommandOperation,
    pub payload: Vec<u8>,
    pub payload_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandOperation {
    Increment(IncrementOperation),
    Read(ReadOperation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncrementOperation {
    pub delta: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ReadOperation {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InjectFaultControl {
    /// Fault admission is the oracle monotonic timestamp immediately after the
    /// complete control line is flushed. Candidates do not forge an ack.
    pub fault_id: DecimalU64,
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub expected_replacement_incarnation: IncarnationToken,
    pub replacement_logical_version: String,
    pub replacement_build_id: String,
    pub replacement_artifact_sha256: String,
    pub fault: FaultKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    Trap(TrapFault),
    CpuHog(CpuHogFault),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TrapFault {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CpuHogFault {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutControl {
    pub rollout_id: String,
    pub targets: Vec<TenantId>,
    pub from_version: String,
    pub to_version: String,
    pub to_build_id: String,
    pub artifact_ref: String,
    pub artifact_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotControl {
    pub tenant_id: TenantId,
    pub expected_incarnation: Option<IncarnationToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetDequeueGateControl {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityProbeControl {
    pub attempt_id: DecimalU64,
    pub probe: AuthorityProbeKind,
    pub artifact_sha256: String,
    pub artifact_bytes: Vec<u8>,
    pub parameter_sha256: String,
    pub production_policy_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityProbeKind {
    WasiP1FsRead,
    WasiP1FsMutate,
    LunaticTcp,
    LunaticUdp,
    LunaticSqliteCreate,
    ExtismHttp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityProbeStage {
    Compile,
    Instantiate,
    Invoke,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityProbeResult {
    AbsentAtLink,
    PolicyDenied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityProbeErrorClass {
    UnknownImport,
    FilesystemCapabilityDenied,
    NetworkCapabilityDenied,
    DatabaseCapabilityDenied,
    HttpCapabilityDenied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QuiesceControl {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeardownTenantControl {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ShutdownControl {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventEnvelope {
    pub schema_version: SchemaVersion,
    pub event_seq: EventSeq,
    pub request_id: RequestId,
    pub message: EventMessage,
}

impl EventEnvelope {
    pub fn new(
        event_seq: impl Into<EventSeq>,
        request_id: impl Into<RequestId>,
        message: EventMessage,
    ) -> Self {
        Self {
            schema_version: SchemaVersion,
            event_seq: event_seq.into(),
            request_id: request_id.into(),
            message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventMessage {
    Hello(HelloEvent),
    Initialized(InitializedEvent),
    TenantCreated(TenantCreatedEvent),
    TenantReady(TenantReadyEvent),
    CommandAccepted(CommandAcceptedEvent),
    CommandRejected(CommandRejectedEvent),
    CommandCompleted(CommandCompletedEvent),
    CommandFailed(CommandFailedEvent),
    ExecutionStarted(ExecutionStartedEvent),
    ExecutionFailed(ExecutionFailedEvent),
    FailureObserved(FailureObservedEvent),
    RolloutStarted(RolloutStartedEvent),
    ActivationStarted(ActivationStartedEvent),
    RolloutTargetReady(RolloutTargetReadyEvent),
    RolloutCommitted(RolloutCommittedEvent),
    RolloutRolledBack(RolloutRolledBackEvent),
    RolloutInDoubt(RolloutInDoubtEvent),
    RolloutRejected(RolloutRejectedEvent),
    SnapshotPresent(SnapshotPresentEvent),
    SnapshotMissing(SnapshotMissingEvent),
    SnapshotStale(SnapshotStaleEvent),
    DequeueGateSet(DequeueGateSetEvent),
    AuthorityProbeAccepted(AuthorityProbeAcceptedEvent),
    AuthorityProbeTerminal(AuthorityProbeTerminalEvent),
    Quiesced(QuiescedEvent),
    TenantTornDown(TenantTornDownEvent),
    ShutdownComplete(ShutdownCompleteEvent),
    Fatal(FatalEvent),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloEvent {
    pub candidate: CandidateIdentity,
    pub implementation_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitializedEvent {
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantCreatedEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantReadyEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandAcceptedEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub command_id: CommandId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandRejectedEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub command_id: CommandId,
    pub reason: RejectionReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    Backpressure,
    PayloadTooLarge,
    CommandIdConflict,
    StaleIncarnation,
    TenantMissing,
    ShuttingDown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandCompletedEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub command_id: CommandId,
    pub result: CommandResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandFailedEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub command_id: CommandId,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandResult {
    pub incarnation: IncarnationToken,
    pub generation: DecimalU64,
    pub counter: i64,
    pub logical_version: String,
    pub build_id: String,
    pub deduplicated: bool,
    pub business_result_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStartedEvent {
    pub fault_id: DecimalU64,
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub origin: ExecutionStartOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionFailedEvent {
    pub fault_id: DecimalU64,
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub reason: FaultFailureReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureObservedEvent {
    pub fault_id: DecimalU64,
    pub tenant_id: TenantId,
    pub failed_incarnation: IncarnationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStartOrigin {
    GuestFirstActionObserver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultFailureReason {
    GuestTrap,
    CpuDeadline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutStartedEvent {
    pub rollout_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationStartedEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub logical_version: String,
    pub build_id: String,
    pub artifact_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutTargetReadyEvent {
    pub rollout_id: String,
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub logical_version: String,
    pub build_id: String,
    pub artifact_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutCommittedEvent {
    pub rollout_id: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutRolledBackEvent {
    pub rollout_id: String,
    pub restored_version: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutInDoubtEvent {
    pub rollout_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutRejectedEvent {
    pub rollout_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPresentEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub state: CommandResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotMissingEvent {
    pub tenant_id: TenantId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotStaleEvent {
    pub tenant_id: TenantId,
    pub expected_incarnation: IncarnationToken,
    pub actual_incarnation: IncarnationToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DequeueGateSetEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityProbeAcceptedEvent {
    pub attempt_id: DecimalU64,
    pub probe: AuthorityProbeKind,
    pub artifact_sha256: String,
    pub parameter_sha256: String,
    pub production_policy_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityProbeTerminalEvent {
    pub attempt_id: DecimalU64,
    pub probe: AuthorityProbeKind,
    pub artifact_sha256: String,
    pub parameter_sha256: String,
    pub production_policy_sha256: String,
    pub stage: AuthorityProbeStage,
    pub result: AuthorityProbeResult,
    pub error_class: AuthorityProbeErrorClass,
    /// Digest of the exact guest return bytes. Before invocation this is the
    /// SHA-256 digest of the empty byte string.
    pub guest_return_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QuiescedEvent {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantTornDownEvent {
    pub tenant_id: TenantId,
    pub incarnation: IncarnationToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ShutdownCompleteEvent {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FatalEvent {
    pub code: String,
    pub message: String,
}

pub fn validate_control_envelope(envelope: &ControlEnvelope) -> Result<(), String> {
    validate_serialized_bounds(envelope)?;
    match &envelope.message {
        ControlMessage::Hello(value) => require_nonempty("oracle_name", &value.oracle_name),
        ControlMessage::Init(value) => {
            validate_run_id(&value.run_id)?;
            if value.tenant_capacity != COMPARISON_TENANT_COUNT {
                return Err(format!("tenant_capacity must be {COMPARISON_TENANT_COUNT}"));
            }
            Ok(())
        }
        ControlMessage::CreateTenant(value) => {
            validate_tenant_id(value.tenant_id)?;
            validate_activation_identity(
                &value.logical_version,
                &value.build_id,
                &value.artifact_sha256,
            )
        }
        ControlMessage::Command(value) => {
            validate_tenant_id(value.tenant_id)?;
            if value.payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
                return Err(format!("payload exceeds {MAX_COMMAND_PAYLOAD_BYTES} bytes"));
            }
            validate_sha256("payload_sha256", &value.payload_sha256)?;
            validate_exact_sha256("payload", &value.payload, &value.payload_sha256)
        }
        ControlMessage::InjectFault(value) => {
            validate_tenant_id(value.tenant_id)?;
            validate_activation_identity(
                &value.replacement_logical_version,
                &value.replacement_build_id,
                &value.replacement_artifact_sha256,
            )
        }
        ControlMessage::Rollout(value) => {
            validate_identifier("rollout_id", &value.rollout_id, MAX_ROLLOUT_ID_BYTES)?;
            if value.targets.is_empty() || value.targets.len() > MAX_ROLLOUT_TARGETS {
                return Err(format!(
                    "rollout targets must contain 1..={MAX_ROLLOUT_TARGETS} tenants"
                ));
            }
            let mut unique = value.targets.clone();
            unique.sort_unstable();
            unique.dedup();
            if unique.len() != value.targets.len() {
                return Err("rollout targets contain duplicates".into());
            }
            value
                .targets
                .iter()
                .copied()
                .try_for_each(validate_tenant_id)?;
            validate_identifier(
                "from_version",
                &value.from_version,
                MAX_LOGICAL_VERSION_BYTES,
            )?;
            validate_activation_identity(
                &value.to_version,
                &value.to_build_id,
                &value.artifact_sha256,
            )?;
            validate_artifact_ref(&value.artifact_ref)
        }
        ControlMessage::AuthorityProbe(value) => {
            if value.artifact_bytes.len() < 8
                || value.artifact_bytes.len() > MAX_AUTHORITY_ARTIFACT_BYTES
            {
                return Err(format!(
                    "authority artifact must contain 8..={MAX_AUTHORITY_ARTIFACT_BYTES} bytes"
                ));
            }
            if &value.artifact_bytes[..8] != b"\0asm\x01\0\0\0" {
                return Err("authority artifact is not a Wasm v1 module".into());
            }
            validate_sha256("artifact_sha256", &value.artifact_sha256)?;
            validate_exact_sha256(
                "authority artifact",
                &value.artifact_bytes,
                &value.artifact_sha256,
            )?;
            validate_sha256("parameter_sha256", &value.parameter_sha256)?;
            validate_sha256("production_policy_sha256", &value.production_policy_sha256)
        }
        _ => Ok(()),
    }
}

pub fn validate_event_envelope(envelope: &EventEnvelope) -> Result<(), String> {
    validate_serialized_bounds(envelope)?;
    match &envelope.message {
        EventMessage::Hello(value) => {
            require_nonempty("implementation_version", &value.implementation_version)
        }
        EventMessage::Initialized(value) => validate_run_id(&value.run_id),
        EventMessage::CommandCompleted(value) => validate_command_result(&value.result),
        EventMessage::ActivationStarted(value) => validate_activation_identity(
            &value.logical_version,
            &value.build_id,
            &value.artifact_sha256,
        ),
        EventMessage::RolloutStarted(value) => {
            validate_identifier("rollout_id", &value.rollout_id, MAX_ROLLOUT_ID_BYTES)
        }
        EventMessage::RolloutTargetReady(value) => {
            validate_identifier("rollout_id", &value.rollout_id, MAX_ROLLOUT_ID_BYTES)?;
            validate_activation_identity(
                &value.logical_version,
                &value.build_id,
                &value.artifact_sha256,
            )
        }
        EventMessage::RolloutCommitted(value) => {
            validate_identifier("rollout_id", &value.rollout_id, MAX_ROLLOUT_ID_BYTES)?;
            validate_identifier("version", &value.version, MAX_LOGICAL_VERSION_BYTES)
        }
        EventMessage::RolloutRolledBack(value) => {
            validate_identifier("rollout_id", &value.rollout_id, MAX_ROLLOUT_ID_BYTES)?;
            validate_identifier(
                "restored_version",
                &value.restored_version,
                MAX_LOGICAL_VERSION_BYTES,
            )?;
            require_nonempty("reason", &value.reason)
        }
        EventMessage::RolloutInDoubt(value) => {
            validate_identifier("rollout_id", &value.rollout_id, MAX_ROLLOUT_ID_BYTES)?;
            require_nonempty("reason", &value.reason)
        }
        EventMessage::RolloutRejected(value) => {
            validate_identifier("rollout_id", &value.rollout_id, MAX_ROLLOUT_ID_BYTES)?;
            require_nonempty("reason", &value.reason)
        }
        EventMessage::SnapshotPresent(value) => validate_command_result(&value.state),
        EventMessage::AuthorityProbeAccepted(value) => {
            validate_sha256("artifact_sha256", &value.artifact_sha256)?;
            validate_sha256("parameter_sha256", &value.parameter_sha256)?;
            validate_sha256("production_policy_sha256", &value.production_policy_sha256)
        }
        EventMessage::AuthorityProbeTerminal(value) => {
            validate_sha256("artifact_sha256", &value.artifact_sha256)?;
            validate_sha256("parameter_sha256", &value.parameter_sha256)?;
            validate_sha256("production_policy_sha256", &value.production_policy_sha256)?;
            validate_sha256("guest_return_sha256", &value.guest_return_sha256)?;
            validate_authority_terminal(value)
        }
        _ => Ok(()),
    }
}

fn validate_command_result(value: &CommandResult) -> Result<(), String> {
    validate_identifier(
        "logical_version",
        &value.logical_version,
        MAX_LOGICAL_VERSION_BYTES,
    )?;
    validate_identifier("build_id", &value.build_id, MAX_BUILD_ID_BYTES)?;
    validate_sha256("business_result_sha256", &value.business_result_sha256)
}

fn validate_activation_identity(
    version: &str,
    build_id: &str,
    artifact_sha256: &str,
) -> Result<(), String> {
    validate_identifier("logical_version", version, MAX_LOGICAL_VERSION_BYTES)?;
    validate_identifier("build_id", build_id, MAX_BUILD_ID_BYTES)?;
    validate_sha256("artifact_sha256", artifact_sha256)
}

fn validate_authority_terminal(value: &AuthorityProbeTerminalEvent) -> Result<(), String> {
    match (value.result, value.stage, value.error_class) {
        (
            AuthorityProbeResult::AbsentAtLink,
            AuthorityProbeStage::Compile | AuthorityProbeStage::Instantiate,
            AuthorityProbeErrorClass::UnknownImport,
        ) => {
            if value.guest_return_sha256 != EMPTY_SHA256 {
                return Err("absent_at_link must use the empty guest-return digest".into());
            }
        }
        (
            AuthorityProbeResult::PolicyDenied,
            AuthorityProbeStage::Instantiate | AuthorityProbeStage::Invoke,
            error,
        ) => {
            let expected = match value.probe {
                AuthorityProbeKind::WasiP1FsRead | AuthorityProbeKind::WasiP1FsMutate => {
                    AuthorityProbeErrorClass::FilesystemCapabilityDenied
                }
                AuthorityProbeKind::LunaticTcp | AuthorityProbeKind::LunaticUdp => {
                    AuthorityProbeErrorClass::NetworkCapabilityDenied
                }
                AuthorityProbeKind::LunaticSqliteCreate => {
                    AuthorityProbeErrorClass::DatabaseCapabilityDenied
                }
                AuthorityProbeKind::ExtismHttp => AuthorityProbeErrorClass::HttpCapabilityDenied,
            };
            if error != expected {
                return Err("policy_denied error class does not match probe".into());
            }
            if value.stage != AuthorityProbeStage::Invoke
                && value.guest_return_sha256 != EMPTY_SHA256
            {
                return Err("a pre-invoke denial must use the empty guest-return digest".into());
            }
        }
        _ => {
            return Err("impossible authority stage/result/error_class combination".into());
        }
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<(), String> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(format!(
            "{label} must be exactly 64 lowercase hexadecimal characters"
        ))
    }
}

fn validate_exact_sha256(label: &str, bytes: &[u8], expected: &str) -> Result<(), String> {
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{label} SHA-256 does not match its bytes"))
    }
}

fn validate_identifier(label: &str, value: &str, maximum: usize) -> Result<(), String> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
    {
        Err(format!(
            "{label} must be a nonempty <= {maximum}-byte ASCII identifier"
        ))
    } else {
        Ok(())
    }
}

pub fn validate_run_id(value: &str) -> Result<(), String> {
    let mut bytes = value.bytes();
    let first = bytes
        .next()
        .ok_or_else(|| "run_id must match ^[a-z0-9][a-z0-9._-]{0,127}$".to_owned())?;
    if value.len() > MAX_RUN_ID_BYTES
        || !(first.is_ascii_lowercase() || first.is_ascii_digit())
        || !bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err("run_id must match ^[a-z0-9][a-z0-9._-]{0,127}$".into());
    }
    Ok(())
}

fn validate_artifact_ref(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > MAX_ARTIFACT_REF_BYTES
        || value.starts_with('/')
        || value.contains('\\')
        || value.as_bytes().get(1) == Some(&b':')
        || value
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        Err("artifact_ref must be a canonical bounded relative reference".into())
    } else {
        Ok(())
    }
}

fn validate_tenant_id(value: TenantId) -> Result<(), String> {
    if value.0 < COMPARISON_TENANT_COUNT {
        Ok(())
    } else {
        Err(format!(
            "tenant_id must be in 0..{}",
            COMPARISON_TENANT_COUNT - 1
        ))
    }
}

fn require_nonempty(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        Err(format!("{label} must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_serialized_bounds<T: Serialize>(value: &T) -> Result<(), String> {
    fn walk(value: &serde_json::Value) -> Result<(), String> {
        match value {
            serde_json::Value::String(value) if value.len() > MAX_WIRE_STRING_BYTES => {
                Err(format!("wire string exceeds {MAX_WIRE_STRING_BYTES} bytes"))
            }
            serde_json::Value::Array(values) => values.iter().try_for_each(walk),
            serde_json::Value::Object(values) => values.iter().try_for_each(|(key, value)| {
                if key.len() > MAX_WIRE_STRING_BYTES {
                    return Err("wire object key is oversized".into());
                }
                if key == "tenant_id"
                    && value
                        .as_u64()
                        .is_none_or(|tenant| tenant >= u64::from(COMPARISON_TENANT_COUNT))
                {
                    return Err(format!(
                        "tenant_id must be in 0..{}",
                        COMPARISON_TENANT_COUNT - 1
                    ));
                }
                walk(value)
            }),
            _ => Ok(()),
        }
    }

    let value = serde_json::to_value(value).map_err(|error| error.to_string())?;
    walk(&value)
}

pub fn decode_control_line(line: &str) -> Result<ControlEnvelope, serde_json::Error> {
    let envelope = crate::wire_bounds::decode_bounded(line)?;
    validate_control_envelope(&envelope).map_err(<serde_json::Error as de::Error>::custom)?;
    Ok(envelope)
}

pub fn decode_event_line(line: &str) -> Result<EventEnvelope, serde_json::Error> {
    let envelope = crate::wire_bounds::decode_bounded(line)?;
    validate_event_envelope(&envelope).map_err(<serde_json::Error as de::Error>::custom)?;
    Ok(envelope)
}
