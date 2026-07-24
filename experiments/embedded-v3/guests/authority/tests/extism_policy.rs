use embedded_v3_authority_guests::{artifact_bytes, Parameters, Probe};
use extism::{Manifest, PluginBuilder, Wasm};

#[test]
fn extism_1_30_links_the_real_http_abi_and_denies_an_unlisted_host() {
    let wasm = Wasm::data(artifact_bytes(Probe::ExtismHttp, &Parameters::default()).unwrap());
    let manifest = Manifest::new([wasm]);
    let mut plugin = PluginBuilder::new(manifest)
        .with_wasi(false)
        .build()
        .expect("the exact Extism 1.30 ABI must link");
    let error = plugin
        .call::<&[u8], Vec<u8>>("invoke", &[])
        .expect_err("no allowed_hosts policy must deny the genuine HTTP request");
    let message = format!("{:#}", error);
    assert!(
        message.contains("HTTP request to")
            && message.contains("is not allowed")
            && message.contains("embedded-v3-authority-canary-0001"),
        "unexpected Extism policy error: {}",
        message
    );
}
