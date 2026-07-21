use anyhow::{anyhow, Context, Result};
use std::{fmt::Display, future::Future, io::Write, pin::Pin};
use wasmtime::{Caller, Linker, Memory, ToWasmtimeResult as _, Val, WasmRet, WasmTy};

pub mod audit;

pub use audit::{
    audit_stats, close_audit, emit_audit_event, flush_audit, global_audit_dispatcher,
    install_global_audit_dispatcher, AuditAction, AuditConfig, AuditDispatcher, AuditEmitOutcome,
    AuditEvent, AuditEventV1, AuditFlushOutcome, AuditReason, AuditResult, AuditSink, AuditStats,
    AuditSubject, AuditTarget, AuditTargetKind, LogAuditSink, SensitiveData, AUDIT_SCHEMA_VERSION,
};

/// Compatibility helpers for Lunatic's legacy numbered async linker calls.
///
/// New Wasmtime releases accept all WebAssembly parameters as one tuple via
/// `Linker::func_wrap_async`. Lunatic's host APIs still use the older,
/// arity-specific callback shape, so these methods keep the migration local
/// while delegating directly to the supported tuple-based API.
macro_rules! declare_linker_async_wrapper {
    ($name:ident, $($ty:ident),+ $(,)?) => {
        fn $name<$($ty,)+ R>(
            &mut self,
            module: &str,
            name: &str,
            func: impl for<'a> Fn(Caller<'a, T>, $($ty),+) -> Box<dyn Future<Output = Result<R>> + Send + 'a>
            + Send
            + Sync
            + 'static,
        ) -> wasmtime::Result<&mut Self>
        where
            $($ty: WasmTy,)+
            R: WasmRet + 'static;
    };
}

pub trait LinkerAsyncExt<T: Send + 'static> {
    declare_linker_async_wrapper!(func_wrap1_async, A1);
    declare_linker_async_wrapper!(func_wrap2_async, A1, A2);
    declare_linker_async_wrapper!(func_wrap3_async, A1, A2, A3);
    declare_linker_async_wrapper!(func_wrap4_async, A1, A2, A3, A4);
    declare_linker_async_wrapper!(func_wrap5_async, A1, A2, A3, A4, A5);
    declare_linker_async_wrapper!(func_wrap6_async, A1, A2, A3, A4, A5, A6);
    declare_linker_async_wrapper!(func_wrap7_async, A1, A2, A3, A4, A5, A6, A7);
    declare_linker_async_wrapper!(func_wrap8_async, A1, A2, A3, A4, A5, A6, A7, A8);
    declare_linker_async_wrapper!(func_wrap9_async, A1, A2, A3, A4, A5, A6, A7, A8, A9);
    declare_linker_async_wrapper!(func_wrap10_async, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10);
    declare_linker_async_wrapper!(
        func_wrap11_async,
        A1,
        A2,
        A3,
        A4,
        A5,
        A6,
        A7,
        A8,
        A9,
        A10,
        A11
    );
}

macro_rules! implement_linker_async_wrapper {
    ($name:ident, $(($ty:ident, $arg:ident)),+ $(,)?) => {
        fn $name<$($ty,)+ R>(
            &mut self,
            module: &str,
            name: &str,
            func: impl for<'a> Fn(Caller<'a, T>, $($ty),+) -> Box<dyn Future<Output = Result<R>> + Send + 'a>
            + Send
            + Sync
            + 'static,
        ) -> wasmtime::Result<&mut Self>
        where
            $($ty: WasmTy,)+
            R: WasmRet + 'static,
        {
            self.func_wrap_async(
                module,
                name,
                move |caller, ($($arg,)+): ($($ty,)+)| {
                    let future = Box::into_pin(func(caller, $($arg),+));
                    Box::new(async move { future.await.to_wasmtime_result() })
                },
            )
        }
    };
}

impl<T: Send + 'static> LinkerAsyncExt<T> for Linker<T> {
    implement_linker_async_wrapper!(func_wrap1_async, (A1, a1));
    implement_linker_async_wrapper!(func_wrap2_async, (A1, a1), (A2, a2));
    implement_linker_async_wrapper!(func_wrap3_async, (A1, a1), (A2, a2), (A3, a3));
    implement_linker_async_wrapper!(func_wrap4_async, (A1, a1), (A2, a2), (A3, a3), (A4, a4));
    implement_linker_async_wrapper!(
        func_wrap5_async,
        (A1, a1),
        (A2, a2),
        (A3, a3),
        (A4, a4),
        (A5, a5)
    );
    implement_linker_async_wrapper!(
        func_wrap6_async,
        (A1, a1),
        (A2, a2),
        (A3, a3),
        (A4, a4),
        (A5, a5),
        (A6, a6)
    );
    implement_linker_async_wrapper!(
        func_wrap7_async,
        (A1, a1),
        (A2, a2),
        (A3, a3),
        (A4, a4),
        (A5, a5),
        (A6, a6),
        (A7, a7)
    );
    implement_linker_async_wrapper!(
        func_wrap8_async,
        (A1, a1),
        (A2, a2),
        (A3, a3),
        (A4, a4),
        (A5, a5),
        (A6, a6),
        (A7, a7),
        (A8, a8)
    );
    implement_linker_async_wrapper!(
        func_wrap9_async,
        (A1, a1),
        (A2, a2),
        (A3, a3),
        (A4, a4),
        (A5, a5),
        (A6, a6),
        (A7, a7),
        (A8, a8),
        (A9, a9)
    );
    implement_linker_async_wrapper!(
        func_wrap10_async,
        (A1, a1),
        (A2, a2),
        (A3, a3),
        (A4, a4),
        (A5, a5),
        (A6, a6),
        (A7, a7),
        (A8, a8),
        (A9, a9),
        (A10, a10)
    );
    implement_linker_async_wrapper!(
        func_wrap11_async,
        (A1, a1),
        (A2, a2),
        (A3, a3),
        (A4, a4),
        (A5, a5),
        (A6, a6),
        (A7, a7),
        (A8, a8),
        (A9, a9),
        (A10, a10),
        (A11, a11)
    );
}

const ALLOCATOR_FUNCTION_NAME: &str = "lunatic_alloc";
const FREEING_FUNCTION_NAME: &str = "lunatic_free";

// Get exported memory
pub fn get_memory<T>(caller: &mut Caller<T>) -> Result<Memory> {
    caller
        .get_export("memory")
        .or_trap("No export `memory` found")?
        .into_memory()
        .or_trap("Export `memory` is not a memory")
}

// Call guest to allocate a Vec of size `size`
pub fn allocate_guest_memory<'a, T: Send>(
    caller: &'a mut Caller<T>,
    size: u32,
) -> Pin<Box<dyn Future<Output = Result<u32>> + Send + 'a>> {
    Box::pin(async move {
        let mut results = [Val::I32(0)];
        caller
            .get_export(ALLOCATOR_FUNCTION_NAME)
            .or_trap(format!("no export named {ALLOCATOR_FUNCTION_NAME} found"))?
            .into_func()
            .or_trap("cannot turn export into func")?
            .call_async(caller, &[Val::I32(size as i32)], &mut results)
            .await
            .or_trap(format!("failed to call {ALLOCATOR_FUNCTION_NAME}"))?;

        Ok(results[0]
            .i32()
            .or_trap(format!("result of {ALLOCATOR_FUNCTION_NAME} is not i32"))? as u32)
    })
}

// Call guest to free a slice of memory at location ptr
pub fn free_guest_memory<'a, T: Send>(
    caller: &'a mut Caller<T>,
    ptr: u32,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let mut results = [];
        let result = caller
            .get_export(FREEING_FUNCTION_NAME)
            .or_trap(format!("no export named {FREEING_FUNCTION_NAME} found"))?
            .into_func()
            .or_trap("cannot turn export into func")?
            .call_async(caller, &[Val::I32(ptr as i32)], &mut results)
            .await;

        result.or_trap(format!("failed to call {FREEING_FUNCTION_NAME}"))?;
        Ok(())
    })
}

// Allocates and writes data to guest memory, updating the len_ptr and returning the allocated ptr.
pub async fn write_to_guest_vec<T: Send>(
    caller: &mut Caller<'_, T>,
    memory: &Memory,
    data: &[u8],
    len_ptr: u32,
) -> Result<u32> {
    let alloc_len = data.len();
    let alloc_ptr = allocate_guest_memory(caller, alloc_len as u32).await?;

    let (memory_slice, _) = memory.data_and_store_mut(&mut (*caller));
    let mut alloc_vec = memory_slice
        .get_mut(alloc_ptr as usize..(alloc_ptr as usize + alloc_len))
        .context("allocated memory does not exist")?;

    alloc_vec.write_all(data)?;

    memory.write(caller, len_ptr as usize, &alloc_len.to_le_bytes())?;

    Ok(alloc_ptr)
}

pub trait IntoTrap<T> {
    fn or_trap<S: Display>(self, info: S) -> Result<T>;
}

impl<T, E: Display> IntoTrap<T> for Result<T, E> {
    fn or_trap<S: Display>(self, info: S) -> Result<T> {
        match self {
            Ok(result) => Ok(result),
            Err(error) => Err(anyhow!(
                "Trap raised during host call: {} ({}).",
                error,
                info
            )),
        }
    }
}

impl<T> IntoTrap<T> for Option<T> {
    fn or_trap<S: Display>(self, info: S) -> Result<T> {
        match self {
            Some(result) => Ok(result),
            None => Err(anyhow!(
                "Trap raised during host call: Expected `Some({})` got `None` ({}).",
                std::any::type_name::<T>(),
                info
            )),
        }
    }
}
