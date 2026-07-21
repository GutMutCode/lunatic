use anyhow::{Context, Result};

const COUNTER_V1: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "_start"))
        (func (export "get_count") (result i32)
            i32.const 0
            i32.load)
        (func (export "increment") (result i32)
            i32.const 0
            i32.const 0
            i32.load
            i32.const 1
            i32.add
            i32.store
            i32.const 0
            i32.load))
"#;

const COUNTER_V2: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "_start"))
        (func (export "get_count") (result i32)
            i32.const 0
            i32.load)
        (func (export "increment") (result i32)
            i32.const 0
            i32.const 0
            i32.load
            i32.const 2
            i32.add
            i32.store
            i32.const 0
            i32.load)
        (func (export "reset")
            i32.const 0
            i32.const 0
            i32.store))
"#;

#[tokio::test]
async fn memory_state_survives_module_replacement() -> Result<()> {
    let config = wasmtime::Config::new();

    let engine = wasmtime::Engine::new(&config)?;
    let module_v1 = wasmtime::Module::new(&engine, COUNTER_V1.as_bytes())?;
    let module_v2 = wasmtime::Module::new(&engine, COUNTER_V2.as_bytes())?;
    let linker = wasmtime::Linker::<()>::new(&engine);

    let mut store_v1 = wasmtime::Store::new(&engine, ());
    let instance_v1 = linker.instantiate_async(&mut store_v1, &module_v1).await?;
    instance_v1
        .get_typed_func::<(), ()>(&mut store_v1, "_start")?
        .call_async(&mut store_v1, ())
        .await?;

    let increment_v1 = instance_v1.get_typed_func::<(), i32>(&mut store_v1, "increment")?;
    for expected in 1..=3 {
        assert_eq!(increment_v1.call_async(&mut store_v1, ()).await?, expected);
    }

    let memory_v1 = instance_v1
        .get_memory(&mut store_v1, "memory")
        .context("v1 memory export not found")?;
    let memory_snapshot = memory_v1.data(&store_v1).to_vec();

    let mut store_v2 = wasmtime::Store::new(&engine, ());
    let instance_v2 = linker.instantiate_async(&mut store_v2, &module_v2).await?;
    let memory_v2 = instance_v2
        .get_memory(&mut store_v2, "memory")
        .context("v2 memory export not found")?;
    memory_v2.data_mut(&mut store_v2)[..memory_snapshot.len()].copy_from_slice(&memory_snapshot);

    let get_count_v2 = instance_v2.get_typed_func::<(), i32>(&mut store_v2, "get_count")?;
    assert_eq!(get_count_v2.call_async(&mut store_v2, ()).await?, 3);

    let increment_v2 = instance_v2.get_typed_func::<(), i32>(&mut store_v2, "increment")?;
    assert_eq!(increment_v2.call_async(&mut store_v2, ()).await?, 5);

    instance_v2
        .get_typed_func::<(), ()>(&mut store_v2, "reset")?
        .call_async(&mut store_v2, ())
        .await?;
    assert_eq!(get_count_v2.call_async(&mut store_v2, ()).await?, 0);
    Ok(())
}
