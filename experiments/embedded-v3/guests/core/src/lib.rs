use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

pub const ABI_VERSION: u32 = 2;
pub const TEMPLATE: &str = include_str!("../templates/tenant.wat.in");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    A,
    B,
    BadA,
    BadB,
}

#[derive(Debug, Clone, Copy)]
pub struct VariantSpec {
    pub variant: Variant,
    pub file_name: &'static str,
    pub version: i32,
    pub build_marker: i64,
    pub bad_tenant: Option<i32>,
}

pub const VARIANTS: [VariantSpec; 4] = [
    VariantSpec {
        variant: Variant::A,
        file_name: "tenant-a.wasm",
        version: 1,
        build_marker: 0x4133_5633_544e_5401,
        bad_tenant: None,
    },
    VariantSpec {
        variant: Variant::B,
        file_name: "tenant-b.wasm",
        version: 2,
        build_marker: 0x4233_5633_544e_5402,
        bad_tenant: None,
    },
    VariantSpec {
        variant: Variant::BadA,
        file_name: "tenant-bad-a.wasm",
        version: 1,
        build_marker: 0x4133_5633_4241_4401,
        bad_tenant: Some(7),
    },
    VariantSpec {
        variant: Variant::BadB,
        file_name: "tenant-bad-b.wasm",
        version: 2,
        build_marker: 0x4233_5633_4241_4402,
        bad_tenant: Some(7),
    },
];

pub fn spec(variant: Variant) -> &'static VariantSpec {
    VARIANTS
        .iter()
        .find(|candidate| candidate.variant == variant)
        .expect("every Variant has a frozen specification")
}

pub fn render_wat(variant: Variant) -> String {
    let spec = spec(variant);
    let guard = match spec.bad_tenant {
        Some(tenant) => format!(
            "(if (i32.eq (local.get $tenant) (i32.const {tenant}))\n      (then unreachable))"
        ),
        None => "(nop)".to_owned(),
    };
    TEMPLATE
        .replace("__VERSION__", &spec.version.to_string())
        .replace("__BUILD_MARKER__", &format!("0x{:016x}", spec.build_marker))
        .replace("__ACTIVATION_GUARD__", &guard)
}

pub fn artifact_bytes(variant: Variant) -> Result<Vec<u8>, wat::Error> {
    wat::parse_str(render_wat(variant))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub fn build_all(root: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let artifact_dir = root.join("artifacts");
    fs::create_dir_all(&artifact_dir)?;

    let template_hash = sha256_hex(TEMPLATE.as_bytes());
    let mut records = Vec::with_capacity(VARIANTS.len());
    for spec in VARIANTS {
        let bytes = artifact_bytes(spec.variant)?;
        let path = artifact_dir.join(spec.file_name);
        write_if_changed(&path, &bytes)?;
        records.push((spec, bytes.len(), sha256_hex(&bytes)));
    }

    let mut manifest = String::new();
    writeln!(&mut manifest, "{{")?;
    writeln!(&mut manifest, "  \"schema_version\": 1,")?;
    writeln!(&mut manifest, "  \"abi_version\": {ABI_VERSION},")?;
    writeln!(&mut manifest, "  \"source\": {{")?;
    writeln!(&mut manifest, "    \"path\": \"templates/tenant.wat.in\",")?;
    writeln!(&mut manifest, "    \"sha256\": \"{template_hash}\"")?;
    writeln!(&mut manifest, "  }},")?;
    writeln!(&mut manifest, "  \"recipe\": {{")?;
    writeln!(&mut manifest, "    \"wat_crate\": \"1.254.0\",")?;
    writeln!(
        &mut manifest,
        "    \"command\": \"cargo run --locked --bin build-core-guests\""
    )?;
    writeln!(&mut manifest, "  }},")?;
    writeln!(&mut manifest, "  \"artifacts\": [")?;
    for (index, (spec, size, digest)) in records.iter().enumerate() {
        let comma = if index + 1 == records.len() { "" } else { "," };
        let bad_tenant = spec
            .bad_tenant
            .map_or_else(|| "null".to_owned(), |tenant| tenant.to_string());
        writeln!(&mut manifest, "    {{")?;
        writeln!(&mut manifest, "      \"file\": \"{}\",", spec.file_name)?;
        writeln!(&mut manifest, "      \"sha256\": \"{digest}\",")?;
        writeln!(&mut manifest, "      \"bytes\": {size},")?;
        writeln!(&mut manifest, "      \"version\": {},", spec.version)?;
        writeln!(
            &mut manifest,
            "      \"build_marker\": \"0x{:016x}\",",
            spec.build_marker
        )?;
        writeln!(&mut manifest, "      \"bad_tenant\": {bad_tenant}")?;
        writeln!(&mut manifest, "    }}{comma}")?;
    }
    writeln!(&mut manifest, "  ]")?;
    writeln!(&mut manifest, "}}")?;

    let manifest_path = artifact_dir.join("manifest.json");
    write_if_changed(&manifest_path, manifest.as_bytes())?;
    Ok(manifest_path)
}

fn write_if_changed(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if fs::read(path).ok().as_deref() == Some(bytes) {
        return Ok(());
    }
    fs::write(path, bytes)
}
