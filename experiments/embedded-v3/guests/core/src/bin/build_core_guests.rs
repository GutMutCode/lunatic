use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = embedded_v3_core_guests::build_all(root)?;
    println!("{}", manifest.display());
    Ok(())
}
