use embedded_v3_oracle::protocol::*;
use sha2::{Digest, Sha256};

#[test]
fn schema_version_three_and_unknown_fields_are_strict() {
    let valid = r#"{
        "schema_version":3,
        "request_id":"1",
        "message":{"hello":{"oracle_name":"oracle","expected_candidate":"extism"}}
    }"#;
    let decoded = decode_control_line(valid).unwrap();
    assert!(matches!(
        decoded.message,
        ControlMessage::Hello(HelloControl {
            expected_candidate: CandidateIdentity::Production(CandidateKind::Extism),
            ..
        })
    ));

    let wrong_version = valid.replace("\"schema_version\":3", "\"schema_version\":2");
    assert!(decode_control_line(&wrong_version)
        .unwrap_err()
        .to_string()
        .contains("unsupported schema_version"));

    let unknown_envelope = valid.replace(
        "\"request_id\":\"1\",",
        "\"request_id\":\"1\",\"extra\":true,",
    );
    assert!(decode_control_line(&unknown_envelope).is_err());

    let unknown_body = valid.replace(
        "\"oracle_name\":\"oracle\"",
        "\"oracle_name\":\"oracle\",\"extra\":true",
    );
    assert!(decode_control_line(&unknown_body).is_err());
}

#[test]
fn event_envelope_and_nested_payload_reject_unknown_fields() {
    let valid = r#"{
        "schema_version":3,
        "event_seq":"1",
        "request_id":"1",
        "message":{"hello":{"candidate":"lunatic","implementation_version":"x"}}
    }"#;
    assert!(decode_event_line(valid).is_ok());
    assert!(decode_event_line(
        &valid.replace("\"event_seq\":\"1\",", "\"event_seq\":\"1\",\"unowned\":0,")
    )
    .is_err());
    assert!(decode_event_line(&valid.replace(
        "\"implementation_version\":\"x\"",
        "\"implementation_version\":\"x\",\"unowned\":0"
    ))
    .is_err());
}

#[test]
fn all_u64_wire_values_are_canonical_decimal_strings() {
    for invalid in [r#"""#, r#""01""#, r#""+1""#, r#"" 1""#, r#""-1""#, "1"] {
        assert!(
            serde_json::from_str::<IncarnationToken>(invalid).is_err(),
            "accepted {}",
            invalid
        );
        assert!(
            serde_json::from_str::<CommandId>(invalid).is_err(),
            "accepted {}",
            invalid
        );
    }
    let exact = serde_json::from_str::<CommandId>(r#""9007199254740993""#).unwrap();
    assert_eq!(exact, CommandId(9_007_199_254_740_993));
}

#[test]
fn oversized_nested_wire_string_is_rejected() {
    let value = "x".repeat(MAX_WIRE_STRING_BYTES + 1);
    let line = format!(
        r#"{{"schema_version":3,"event_seq":"1","request_id":"1","message":{{"hello":{{"candidate":"fake","implementation_version":"{value}"}}}}}}"#
    );
    assert!(decode_event_line(&line).is_err());
}

#[test]
fn invalid_json_is_rejected_before_the_automaton() {
    assert!(decode_event_line("{definitely not NDJSON}").is_err());
}

#[test]
fn fake_identity_is_not_a_production_candidate() {
    assert!("fake".parse::<CandidateKind>().is_err());
    let line = r#"{
        "schema_version":3,
        "event_seq":"1",
        "request_id":"1",
        "message":{"hello":{"candidate":"fake","implementation_version":"test"}}
    }"#;
    let decoded = decode_event_line(line).unwrap();
    assert!(matches!(
        decoded.message,
        EventMessage::Hello(HelloEvent {
            candidate: CandidateIdentity::TestDouble(TestDoubleKind::Fake),
            ..
        })
    ));
}

#[test]
fn digests_and_artifact_bytes_are_validated_during_decode() {
    let invalid_digest = r#"{
        "schema_version":3,
        "request_id":"1",
        "message":{"create_tenant":{
            "tenant_id":0,
            "incarnation":"0",
            "logical_version":"A",
            "build_id":"A",
            "artifact_sha256":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        }}
    }"#;
    assert!(decode_control_line(invalid_digest).is_err());

    let mismatched_authority_artifact = r#"{
        "schema_version":3,
        "request_id":"1",
        "message":{"authority_probe":{
            "attempt_id":"1",
            "probe":"wasi_p1_fs_read",
            "artifact_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "artifact_bytes":[0,97,115,109,1,0,0,0],
            "parameter_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "production_policy_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        }}
    }"#;
    assert!(decode_control_line(mismatched_authority_artifact).is_err());
}

#[test]
fn generic_guest_trap_is_not_an_authority_outcome() {
    let obsolete = r#"{
        "schema_version":3,
        "event_seq":"1",
        "request_id":"1",
        "message":{"authority_denied":{"canary_id":"old","denial":"guest_trap"}}
    }"#;
    assert!(decode_event_line(obsolete).is_err());
}

#[test]
fn tenant_capacity_and_ids_are_closed_to_the_frozen_population() {
    let wrong_capacity = ControlEnvelope::new(
        1,
        ControlMessage::Init(InitControl {
            run_id: "run".into(),
            tenant_capacity: COMPARISON_TENANT_COUNT - 1,
        }),
    );
    assert!(validate_control_envelope(&wrong_capacity).is_err());

    let out_of_range_event = EventEnvelope::new(
        1,
        1,
        EventMessage::TenantReady(TenantReadyEvent {
            tenant_id: TenantId(COMPARISON_TENANT_COUNT),
            incarnation: IncarnationToken::new(0),
        }),
    );
    assert!(validate_event_envelope(&out_of_range_event).is_err());

    let decoded = r#"{
        "schema_version":3,
        "request_id":"1",
        "message":{"create_tenant":{
            "tenant_id":32,
            "incarnation":"0",
            "logical_version":"A",
            "build_id":"A",
            "artifact_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }}
    }"#;
    assert!(decode_control_line(decoded).is_err());
}

#[test]
fn wire_admits_only_the_limit_and_single_oversize_probe() {
    let payload = vec![7; MAX_COMMAND_PAYLOAD_BYTES];
    let digest = format!("{:x}", Sha256::digest(&payload));
    let boundary = ControlEnvelope::new(
        1,
        ControlMessage::Command(CommandControl {
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(0),
            command_id: CommandId(1),
            operation: CommandOperation::Read(ReadOperation::default()),
            payload,
            payload_sha256: digest,
        }),
    );
    assert!(validate_control_envelope(&boundary).is_ok());

    let too_large = ControlEnvelope::new(
        2,
        ControlMessage::Command(CommandControl {
            tenant_id: TenantId(0),
            incarnation: IncarnationToken::new(0),
            command_id: CommandId(2),
            operation: CommandOperation::Read(ReadOperation::default()),
            payload: vec![0; MAX_COMMAND_PAYLOAD_BYTES + 1],
            payload_sha256: EMPTY_SHA256.into(),
        }),
    );
    assert!(validate_control_envelope(&too_large).is_err());
}

#[test]
fn artifact_reference_rejects_dot_components() {
    let rollout = ControlEnvelope::new(
        1,
        ControlMessage::Rollout(RolloutControl {
            rollout_id: "r".into(),
            targets: vec![TenantId(0)],
            from_version: "A".into(),
            to_version: "B".into(),
            to_build_id: "B".into(),
            artifact_ref: "./tenant-b.wasm".into(),
            artifact_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
        }),
    );
    assert!(validate_control_envelope(&rollout).is_err());
}

#[test]
fn run_id_matches_the_manifest_schema_at_both_wire_directions() {
    for valid in ["a", "0.x-y_z", &format!("a{}", "z".repeat(127))] {
        assert!(validate_run_id(valid).is_ok(), "{} should be valid", valid);
    }
    for invalid in [
        "",
        "Uppercase",
        "_leading",
        ".leading",
        "slash/path",
        "colon:not-allowed",
        &format!("a{}", "z".repeat(128)),
    ] {
        assert!(
            validate_run_id(invalid).is_err(),
            "{} should be invalid",
            invalid
        );
    }

    let invalid_init = ControlEnvelope::new(
        1,
        ControlMessage::Init(InitControl {
            run_id: "Uppercase".into(),
            tenant_capacity: COMPARISON_TENANT_COUNT,
        }),
    );
    assert!(validate_control_envelope(&invalid_init).is_err());

    let invalid_initialized = EventEnvelope::new(
        1,
        1,
        EventMessage::Initialized(InitializedEvent {
            run_id: "../escape".into(),
        }),
    );
    assert!(validate_event_envelope(&invalid_initialized).is_err());
}

#[test]
fn rollout_id_is_validated_on_control_and_every_terminal_shape() {
    let invalid_control = ControlEnvelope::new(
        1,
        ControlMessage::Rollout(RolloutControl {
            rollout_id: "bad/rollout".into(),
            targets: vec![TenantId(0)],
            from_version: "A".into(),
            to_version: "B".into(),
            to_build_id: "B".into(),
            artifact_ref: "tenant-b.wasm".into(),
            artifact_sha256: "a".repeat(64),
        }),
    );
    assert!(validate_control_envelope(&invalid_control).is_err());

    let invalid_started = EventEnvelope::new(
        1,
        1,
        EventMessage::RolloutStarted(RolloutStartedEvent {
            rollout_id: "".into(),
        }),
    );
    assert!(validate_event_envelope(&invalid_started).is_err());

    let empty_reason = EventEnvelope::new(
        2,
        1,
        EventMessage::RolloutRejected(RolloutRejectedEvent {
            rollout_id: "rollout:1".into(),
            reason: "".into(),
        }),
    );
    assert!(validate_event_envelope(&empty_reason).is_err());
}
