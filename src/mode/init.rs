use std::{
    fs::{create_dir_all, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
};

use anyhow::{anyhow, Result};
use toml::{value::Table, Value};

const WASI_TARGET: &str = "wasm32-wasip1";
const LEGACY_WASI_TARGET: &str = "wasm32-wasi";
const RUNNER: &str = "lunatic run";
const LEGACY_RUNNER: &str = "lunatic";

pub(crate) fn start() -> Result<()> {
    // Check if the current directory is a Rust cargo project.
    if !Path::new("Cargo.toml").exists() {
        return Err(anyhow!("Must be called inside a cargo project"));
    }

    // Open or create cargo config file.
    create_dir_all(".cargo").unwrap();
    let mut config_toml = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(".cargo/config.toml")
        .unwrap();

    let mut content = String::new();
    config_toml.read_to_string(&mut content).unwrap();

    let new_config = update_config(&content)?;

    // Truncate existing config
    config_toml.set_len(0).unwrap();
    config_toml.rewind().unwrap();
    config_toml
        .write_all(new_config.as_bytes())
        .expect("unable to write new config to `.cargo/config.toml`");

    println!("Cargo project initialized!");

    Ok(())
}

fn update_config(content: &str) -> Result<String> {
    let mut content = content.parse::<Value>()?;
    let table = content
        .as_table_mut()
        .ok_or_else(|| anyhow!("wrong `.cargo/config.toml` format"))?;

    set_build_target(table)?;
    set_target_runner(table)?;

    Ok(toml::to_string(table)?)
}

fn set_build_target(table: &mut Table) -> Result<()> {
    match table.get_mut("build") {
        Some(value) => {
            let build = value
                .as_table_mut()
                .ok_or_else(|| anyhow!("wrong `.cargo/config.toml` format"))?;
            match build.get_mut("target") {
                Some(Value::String(target)) if target == LEGACY_WASI_TARGET => {
                    *target = WASI_TARGET.to_owned();
                }
                Some(Value::String(target)) if target != WASI_TARGET => {
                    return Err(anyhow!(
                        "value `build.target` inside `.cargo/config.toml` is `{target}`; expected `{WASI_TARGET}`"
                    ));
                }
                Some(Value::String(_)) => {
                    // The current target is already configured.
                }
                Some(_) => {
                    return Err(anyhow!(
                        "value `build.target` inside `.cargo/config.toml` must be a string"
                    ));
                }
                None => {
                    build.insert("target".to_owned(), Value::String(WASI_TARGET.to_owned()));
                }
            }
        }
        None => {
            let mut new_build = Table::new();
            new_build.insert("target".to_owned(), Value::String(WASI_TARGET.to_owned()));
            table.insert("build".to_owned(), Value::Table(new_build));
        }
    };

    Ok(())
}

fn set_target_runner(table: &mut Table) -> Result<()> {
    if !table.contains_key("target") {
        table.insert("target".to_owned(), Value::Table(Table::new()));
    }

    let targets = table
        .get_mut("target")
        .and_then(Value::as_table_mut)
        .ok_or_else(|| anyhow!("wrong `.cargo/config.toml` format"))?;

    if let Some(legacy) = targets.remove(LEGACY_WASI_TARGET) {
        let mut legacy = match legacy {
            Value::Table(table) => table,
            _ => {
                return Err(anyhow!(
                    "value `target.{LEGACY_WASI_TARGET}` inside `.cargo/config.toml` must be a table"
                ));
            }
        };
        set_runner(&mut legacy, LEGACY_WASI_TARGET)?;

        if let Some(current) = targets.get_mut(WASI_TARGET) {
            let current = current.as_table_mut().ok_or_else(|| {
                anyhow!("value `target.{WASI_TARGET}` inside `.cargo/config.toml` must be a table")
            })?;
            set_runner(current, WASI_TARGET)?;
            merge_legacy_target(current, legacy)?;
        } else {
            targets.insert(WASI_TARGET.to_owned(), Value::Table(legacy));
        }
    }

    if !targets.contains_key(WASI_TARGET) {
        targets.insert(WASI_TARGET.to_owned(), Value::Table(Table::new()));
    }

    let target = targets
        .get_mut(WASI_TARGET)
        .and_then(Value::as_table_mut)
        .ok_or_else(|| {
            anyhow!("value `target.{WASI_TARGET}` inside `.cargo/config.toml` must be a table")
        })?;
    set_runner(target, WASI_TARGET)
}

fn set_runner(target: &mut Table, target_name: &str) -> Result<()> {
    match target.get_mut("runner") {
        Some(Value::String(runner)) if runner == LEGACY_RUNNER => {
            *runner = RUNNER.to_owned();
        }
        Some(Value::String(runner)) if runner != RUNNER => {
            return Err(anyhow!(
                "value `target.{target_name}.runner` inside `.cargo/config.toml` is `{runner}`; expected `{RUNNER}`"
            ));
        }
        Some(Value::String(_)) => {
            // The current runner is already configured.
        }
        Some(_) => {
            return Err(anyhow!(
                "value `target.{target_name}.runner` inside `.cargo/config.toml` must be a string"
            ));
        }
        None => {
            target.insert("runner".to_owned(), Value::String(RUNNER.to_owned()));
        }
    }

    Ok(())
}

fn merge_legacy_target(current: &mut Table, legacy: Table) -> Result<()> {
    for (key, value) in legacy {
        match current.get(&key) {
            Some(current_value) if current_value != &value => {
                return Err(anyhow!(
                    "cannot migrate `target.{LEGACY_WASI_TARGET}.{key}` because `target.{WASI_TARGET}.{key}` has a conflicting value"
                ));
            }
            Some(_) => {}
            None => {
                current.insert(key, value);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn updated_table(content: &str) -> Table {
        update_config(content)
            .unwrap()
            .parse::<Value>()
            .unwrap()
            .as_table()
            .unwrap()
            .clone()
    }

    #[test]
    fn creates_current_target_configuration() {
        let config = updated_table("");

        assert_eq!(config["build"]["target"].as_str(), Some("wasm32-wasip1"));
        assert_eq!(
            config["target"]["wasm32-wasip1"]["runner"].as_str(),
            Some("lunatic run")
        );
    }

    #[test]
    fn migrates_legacy_target_and_preserves_target_options() {
        let config = updated_table(
            r#"
[build]
target = "wasm32-wasi"

[target.wasm32-wasi]
runner = "lunatic"
rustflags = ["-C", "target-feature=+bulk-memory"]
"#,
        );

        assert_eq!(config["build"]["target"].as_str(), Some("wasm32-wasip1"));
        assert!(config["target"].get("wasm32-wasi").is_none());
        assert_eq!(
            config["target"]["wasm32-wasip1"]["runner"].as_str(),
            Some("lunatic run")
        );
        assert_eq!(
            config["target"]["wasm32-wasip1"]["rustflags"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn merges_non_conflicting_legacy_and_current_target_options() {
        let config = updated_table(
            r#"
[build]
target = "wasm32-wasip1"

[target.wasm32-wasip1]
runner = "lunatic run"

[target.wasm32-wasi]
rustflags = ["-C", "target-feature=+bulk-memory"]
"#,
        );

        assert!(config["target"].get("wasm32-wasi").is_none());
        assert!(config["target"]["wasm32-wasip1"].get("rustflags").is_some());
    }

    #[test]
    fn rejects_conflicting_legacy_and_current_target_options() {
        let error = update_config(
            r#"
[build]
target = "wasm32-wasip1"

[target.wasm32-wasip1]
rustflags = ["-C", "opt-level=3"]

[target.wasm32-wasi]
rustflags = ["-C", "opt-level=z"]
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("conflicting value"));
    }

    #[test]
    fn rejects_an_unrelated_build_target() {
        let error = update_config(
            r#"
[build]
target = "wasm32-unknown-unknown"
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("expected `wasm32-wasip1`"));
    }
}
