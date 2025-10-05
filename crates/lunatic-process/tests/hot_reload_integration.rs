use anyhow::Result;

#[tokio::test]
async fn test_basic_memory_snapshot_and_restore() -> Result<()> {
    let v1_wasm = std::fs::read("/tmp/counter_v1.wasm")?;
    let v2_wasm = std::fs::read("/tmp/counter_v2.wasm")?;

    let mut config = wasmtime::Config::new();
    config.async_support(true);
    
    let engine = wasmtime::Engine::new(&config)?;
    let module_v1 = wasmtime::Module::new(&engine, &v1_wasm)?;
    let module_v2 = wasmtime::Module::new(&engine, &v2_wasm)?;

    let linker = wasmtime::Linker::<()>::new(&engine);

    let mut store = wasmtime::Store::new(&engine, ());
    let instance_v1 = linker.instantiate_async(&mut store, &module_v1).await?;

    let _start = instance_v1.get_typed_func::<(), ()>(&mut store, "_start")?;
    _start.call_async(&mut store, ()).await?;

    let increment = instance_v1
        .get_typed_func::<(), i32>(&mut store, "increment")?;
    let get_count = instance_v1
        .get_typed_func::<(), i32>(&mut store, "get_count")?;

    let memory_v1 = instance_v1
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow::anyhow!("memory export not found"))?;
    
    let initial_bytes = &memory_v1.data(&store)[0..4];
    let initial_value = i32::from_le_bytes([initial_bytes[0], initial_bytes[1], initial_bytes[2], initial_bytes[3]]);
    println!("Initial counter value in memory: {}", initial_value);

    let count1 = increment.call_async(&mut store, ()).await?;
    println!("Increment returned: {}", count1);
    
    let bytes_after = &memory_v1.data(&store)[0..4];
    let value_after = i32::from_le_bytes([bytes_after[0], bytes_after[1], bytes_after[2], bytes_after[3]]);
    println!("Counter value in memory after increment: {}", value_after);
    
    assert_eq!(count1, 1, "Counter should be 1 after first increment");

    let count2 = increment.call_async(&mut store, ()).await?;
    assert_eq!(count2, 2, "Counter should be 2 after second increment");

    let count3 = increment.call_async(&mut store, ()).await?;
    assert_eq!(count3, 3, "Counter should be 3 after third increment");

    let memory_v1 = instance_v1
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow::anyhow!("memory export not found"))?;
    
    let memory_snapshot: Vec<u8> = memory_v1.data(&store).to_vec();
    println!("✓ Captured {} bytes of memory", memory_snapshot.len());

    let mut store_v2 = wasmtime::Store::new(&engine, ());
    let instance_v2 = linker.instantiate_async(&mut store_v2, &module_v2).await?;

    let memory_v2 = instance_v2
        .get_memory(&mut store_v2, "memory")
        .ok_or_else(|| anyhow::anyhow!("memory export not found"))?;

    memory_v2.data_mut(&mut store_v2)[..memory_snapshot.len()].copy_from_slice(&memory_snapshot);
    println!("✓ Memory restored successfully");

    let get_count_v2 = instance_v2
        .get_typed_func::<(), i32>(&mut store_v2, "get_count")?;
    
    let count_after_reload = get_count_v2.call_async(&mut store_v2, ()).await?;
    assert_eq!(count_after_reload, 3, "Counter should still be 3 after reload");
    println!("✓ Counter value preserved: {}", count_after_reload);

    let increment_v2 = instance_v2
        .get_typed_func::<(), i32>(&mut store_v2, "increment")?;
    
    let count_v2 = increment_v2.call_async(&mut store_v2, ()).await?;
    assert_eq!(count_v2, 5, "Counter should be 5 (3 + 2) with v2 increment");
    println!("✓ V2 increment works correctly (added 2): {}", count_v2);

    let reset_v2 = instance_v2
        .get_typed_func::<(), ()>(&mut store_v2, "reset")?;
    
    reset_v2.call_async(&mut store_v2, ()).await?;
    let count_after_reset = get_count_v2.call_async(&mut store_v2, ()).await?;
    assert_eq!(count_after_reset, 0, "Counter should be 0 after reset");
    println!("✓ V2 reset function works: {}", count_after_reset);

    Ok(())
}
