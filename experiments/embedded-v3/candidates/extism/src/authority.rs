use anyhow::{bail, Result};
use extism::{Manifest, Plugin, PluginBuilder, Wasm};

use crate::guest::production_host_functions;
use crate::protocol::{
    AuthorityProbeErrorClass, AuthorityProbeKind, AuthorityProbeResult, AuthorityProbeStage,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityOutcome {
    pub stage: AuthorityProbeStage,
    pub result: AuthorityProbeResult,
    pub error_class: AuthorityProbeErrorClass,
}

pub fn run_probe(probe: AuthorityProbeKind, artifact: Vec<u8>) -> Result<AuthorityOutcome> {
    let manifest = Manifest::new([Wasm::data(artifact)]);
    let compiled = match PluginBuilder::new(manifest)
        .with_wasi(false)
        .with_functions(production_host_functions())
        .compile()
    {
        Ok(compiled) => compiled,
        Err(error) => {
            return classify_link_failure(
                probe,
                AuthorityProbeStage::Compile,
                &format!("{error:#}"),
            );
        }
    };
    let mut plugin = match Plugin::new_from_compiled(&compiled) {
        Ok(plugin) => plugin,
        Err(error) => {
            return classify_link_failure(
                probe,
                AuthorityProbeStage::Instantiate,
                &format!("{error:#}"),
            );
        }
    };
    if probe != AuthorityProbeKind::ExtismHttp {
        bail!("authority probe {probe:?} unexpectedly linked through the production builder");
    }
    let error = match plugin.call::<&[u8], Vec<u8>>("invoke", &[]) {
        Ok(_) => bail!("the no-http production build unexpectedly allowed the exact HTTP canary"),
        Err(error) => error,
    };
    let message = format!("{error:#}");
    if !message.contains("http_request is not enabled") || !message.contains("is not allowed") {
        bail!("Extism HTTP invocation failed outside the frozen no-http denial path: {message}");
    }
    Ok(AuthorityOutcome {
        stage: AuthorityProbeStage::Invoke,
        result: AuthorityProbeResult::PolicyDenied,
        error_class: AuthorityProbeErrorClass::HttpCapabilityDenied,
    })
}

fn classify_link_failure(
    probe: AuthorityProbeKind,
    stage: AuthorityProbeStage,
    message: &str,
) -> Result<AuthorityOutcome> {
    if probe == AuthorityProbeKind::ExtismHttp {
        bail!("the exact Extism HTTP ABI failed before its policy check at {stage:?}: {message}");
    }
    let (module, import) = expected_unavailable_import(probe);
    let normalized = message.to_ascii_lowercase();
    if !normalized.contains("unknown import")
        || !message.contains(module)
        || !message.contains(import)
    {
        bail!("probe did not fail with its exact unavailable production import: {message}");
    }
    Ok(AuthorityOutcome {
        stage,
        result: AuthorityProbeResult::AbsentAtLink,
        error_class: AuthorityProbeErrorClass::UnknownImport,
    })
}

fn expected_unavailable_import(probe: AuthorityProbeKind) -> (&'static str, &'static str) {
    match probe {
        AuthorityProbeKind::WasiP1FsRead | AuthorityProbeKind::WasiP1FsMutate => {
            ("wasi_snapshot_preview1", "path_open")
        }
        AuthorityProbeKind::LunaticTcp => ("lunatic::networking", "tcp_connect"),
        AuthorityProbeKind::LunaticUdp => ("lunatic::networking", "udp_bind"),
        AuthorityProbeKind::LunaticSqliteCreate => ("lunatic::sqlite", "open"),
        AuthorityProbeKind::ExtismHttp => ("extism:host/env", "http_request"),
    }
}
