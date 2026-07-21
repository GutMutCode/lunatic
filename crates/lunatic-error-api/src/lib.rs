use anyhow::Result;
use hash_map_id::HashMapId;
use lunatic_common_api::{get_memory, IntoTrap};
use std::{error::Error, fmt};
use wasmtime::{Caller, Linker, ToWasmtimeResult as _};

pub type ErrorResource = HashMapId<anyhow::Error>;

/// Maximum number of guest-visible errors retained by a process.
pub const MAX_ERROR_RESOURCES: usize = 1024;

#[derive(Debug)]
struct ErrorResourceLimitReached;

impl fmt::Display for ErrorResourceLimitReached {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "error resource limit reached; release retained errors with lunatic::error::drop",
        )
    }
}

impl Error for ErrorResourceLimitReached {}

fn is_limit_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<ErrorResourceLimitReached>().is_some()
}

fn limit_error_id(resources: &ErrorResource) -> Option<u64> {
    resources
        .iter()
        .find_map(|(error_id, error)| is_limit_error(error).then_some(*error_id))
}

fn release_error_resource(resources: &mut ErrorResource, error_id: u64) -> bool {
    match resources.get(error_id) {
        Some(error) if is_limit_error(error) => true,
        Some(_) => resources.remove(error_id).is_some(),
        None => false,
    }
}

pub trait ErrorCtx {
    fn error_resources(&self) -> &ErrorResource;
    fn error_resources_mut(&mut self) -> &mut ErrorResource;

    /// Adds a guest-visible error while keeping the per-process table bounded.
    ///
    /// The last slot is reserved for a stable capacity error. Once the table
    /// reaches [`MAX_ERROR_RESOURCES`], later insertions return that existing
    /// capacity-error ID until the guest releases retained errors. Live guest
    /// handles are not evicted by this method.
    fn add_error_resource(&mut self, error: anyhow::Error) -> u64 {
        let resources = self.error_resources_mut();

        if let Some(error_id) = limit_error_id(resources) {
            if resources.len() >= MAX_ERROR_RESOURCES {
                return error_id;
            }
            return resources.add(error);
        }

        if resources.len() >= MAX_ERROR_RESOURCES.saturating_sub(1) {
            // Direct mutation through `error_resources_mut` bypasses this
            // quota. Runtime host APIs use this insertion method exclusively.
            return resources.add(anyhow::Error::new(ErrorResourceLimitReached));
        }

        resources.add(error)
    }
}

// Register the error APIs to the linker
pub fn register<T: ErrorCtx + 'static>(linker: &mut Linker<T>) -> Result<()> {
    linker.func_wrap(
        "lunatic::error",
        "string_size",
        |caller: Caller<'_, T>, error_id: u64| string_size(caller, error_id).to_wasmtime_result(),
    )?;
    linker.func_wrap(
        "lunatic::error",
        "to_string",
        |caller: Caller<'_, T>, error_id: u64, error_str_ptr: u32| {
            to_string(caller, error_id, error_str_ptr).to_wasmtime_result()
        },
    )?;
    linker.func_wrap(
        "lunatic::error",
        "drop",
        |caller: Caller<'_, T>, error_id: u64| drop(caller, error_id).to_wasmtime_result(),
    )?;
    Ok(())
}

// Returns the size of the string representation of the error.
//
// Traps:
// * If the error ID doesn't exist.
fn string_size<T: ErrorCtx>(caller: Caller<T>, error_id: u64) -> Result<u32> {
    let error = caller
        .data()
        .error_resources()
        .get(error_id)
        .or_trap("lunatic::error::string_size")?;
    Ok(error.to_string().len() as u32)
}

// Writes the string representation of the error to the guest memory.
// `lunatic::error::string_size` can be used to get the string size.
//
// Traps:
// * If the error ID doesn't exist.
// * If any memory outside the guest heap space is referenced.
fn to_string<T: ErrorCtx>(mut caller: Caller<T>, error_id: u64, error_str_ptr: u32) -> Result<()> {
    let error = caller
        .data()
        .error_resources()
        .get(error_id)
        .or_trap("lunatic::error::string_size")?;
    let error_str = error.to_string();
    let memory = get_memory(&mut caller)?;
    memory
        .write(&mut caller, error_str_ptr as usize, error_str.as_ref())
        .or_trap("lunatic::error::string_size")?;
    Ok(())
}

// Drops the error resource.
//
// Traps:
// * If the error ID doesn't exist.
fn drop<T: ErrorCtx>(mut caller: Caller<T>, error_id: u64) -> Result<()> {
    release_error_resource(caller.data_mut().error_resources_mut(), error_id)
        .then_some(())
        .or_trap("lunatic::error::drop")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestCtx {
        error_resources: ErrorResource,
    }

    // Only the original required methods are implemented. This ensures the
    // bounded insertion API remains source-compatible for existing contexts.
    impl ErrorCtx for TestCtx {
        fn error_resources(&self) -> &ErrorResource {
            &self.error_resources
        }

        fn error_resources_mut(&mut self) -> &mut ErrorResource {
            &mut self.error_resources
        }
    }

    #[test]
    fn bounded_insertion_retains_at_most_the_limit() {
        let mut context = TestCtx::default();

        for index in 0..=MAX_ERROR_RESOURCES {
            context.add_error_resource(anyhow::anyhow!("error {index}"));
        }

        assert_eq!(context.error_resources().len(), MAX_ERROR_RESOURCES);
    }

    #[test]
    fn bounded_insertion_keeps_live_handles_and_reuses_a_capacity_error() {
        let mut context = TestCtx::default();
        let oldest_error_id = context.add_error_resource(anyhow::anyhow!("oldest"));

        for index in 1..MAX_ERROR_RESOURCES {
            context.add_error_resource(anyhow::anyhow!("error {index}"));
        }

        let capacity_error_id = context.add_error_resource(anyhow::anyhow!("not retained"));
        let repeated_capacity_error_id =
            context.add_error_resource(anyhow::anyhow!("also not retained"));

        assert!(context.error_resources().get(oldest_error_id).is_some());
        assert_eq!(capacity_error_id, repeated_capacity_error_id);
        assert!(release_error_resource(
            context.error_resources_mut(),
            capacity_error_id
        ));
        assert_eq!(
            context
                .error_resources()
                .get(capacity_error_id)
                .map(ToString::to_string)
                .as_deref(),
            Some("error resource limit reached; release retained errors with lunatic::error::drop")
        );
        assert_eq!(
            context.add_error_resource(anyhow::anyhow!("still full")),
            capacity_error_id
        );
        assert_eq!(context.error_resources().len(), MAX_ERROR_RESOURCES);
    }

    #[test]
    fn bounded_insertion_accepts_new_errors_after_an_explicit_drop() {
        let mut context = TestCtx::default();
        for index in 0..=MAX_ERROR_RESOURCES {
            context.add_error_resource(anyhow::anyhow!("error {index}"));
        }
        let dropped_id = 0;
        context.error_resources_mut().remove(dropped_id);
        let replacement_id = context.add_error_resource(anyhow::anyhow!("replacement"));

        assert!(context.error_resources().get(dropped_id).is_none());
        assert_eq!(
            context
                .error_resources()
                .get(replacement_id)
                .map(ToString::to_string)
                .as_deref(),
            Some("replacement")
        );
        assert_eq!(context.error_resources().len(), MAX_ERROR_RESOURCES);
    }
}
