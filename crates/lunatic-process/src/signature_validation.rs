use anyhow::Result;
use wasmtime::{ExportType, ExternType, FuncType, GlobalType, MemoryType, TableType};

use crate::runtimes::wasmtime::WasmtimeCompiledModule;
use crate::state::ProcessState;

#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    MissingExport(String),
    TypeMismatch {
        export: String,
        expected: String,
        got: String,
    },
    MemorySizeMismatch {
        expected: u64,
        got: u64,
    },
    IncompatibleSignature {
        function: String,
        details: String,
    },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::MissingExport(name) => {
                write!(f, "Missing export: '{}'", name)
            }
            ValidationError::TypeMismatch {
                export,
                expected,
                got,
            } => {
                write!(
                    f,
                    "Type mismatch for '{}': expected {}, got {}",
                    export, expected, got
                )
            }
            ValidationError::MemorySizeMismatch { expected, got } => {
                write!(
                    f,
                    "Memory size mismatch: expected {} pages, got {} pages",
                    expected, got
                )
            }
            ValidationError::IncompatibleSignature { function, details } => {
                write!(f, "Incompatible signature for '{}': {}", function, details)
            }
        }
    }
}

pub struct SignatureValidator;

impl SignatureValidator {
    /// Validate that a new module is compatible with an old module for hot reload
    pub fn validate_compatibility<S: ProcessState>(
        old_module: &WasmtimeCompiledModule<S>,
        new_module: &WasmtimeCompiledModule<S>,
    ) -> Result<Vec<ValidationError>> {
        let mut errors = Vec::new();

        let old_exports: Vec<ExportType> = old_module.exports().collect();
        let new_exports: Vec<ExportType> = new_module.exports().collect();

        for old_export in &old_exports {
            let export_name = old_export.name();

            let new_export = new_exports.iter().find(|e| e.name() == export_name);

            if let Some(new_export) = new_export {
                if let Err(err) = Self::validate_export_types(old_export, new_export) {
                    errors.push(err);
                }
            } else {
                errors.push(ValidationError::MissingExport(export_name.to_string()));
            }
        }

        Ok(errors)
    }

    fn validate_export_types(old: &ExportType, new: &ExportType) -> Result<(), ValidationError> {
        let old_type = old.ty();
        let new_type = new.ty();

        match (&old_type, &new_type) {
            (ExternType::Func(old_func), ExternType::Func(new_func)) => {
                Self::validate_func_types(old.name(), old_func, new_func)
            }
            (ExternType::Memory(old_mem), ExternType::Memory(new_mem)) => {
                Self::validate_memory_types(old_mem, new_mem)
            }
            (ExternType::Table(old_table), ExternType::Table(new_table)) => {
                Self::validate_table_types(old_table, new_table)
            }
            (ExternType::Global(old_global), ExternType::Global(new_global)) => {
                Self::validate_global_types(old_global, new_global)
            }
            _ => Err(ValidationError::TypeMismatch {
                export: old.name().to_string(),
                expected: format!("{:?}", old_type),
                got: format!("{:?}", new_type),
            }),
        }
    }

    fn validate_func_types(
        name: &str,
        old: &FuncType,
        new: &FuncType,
    ) -> Result<(), ValidationError> {
        let old_params: Vec<_> = old.params().collect();
        let new_params: Vec<_> = new.params().collect();
        let old_results: Vec<_> = old.results().collect();
        let new_results: Vec<_> = new.results().collect();

        if old_params != new_params {
            return Err(ValidationError::IncompatibleSignature {
                function: name.to_string(),
                details: format!(
                    "Parameter types differ: old {:?}, new {:?}",
                    old_params, new_params
                ),
            });
        }

        if old_results != new_results {
            return Err(ValidationError::IncompatibleSignature {
                function: name.to_string(),
                details: format!(
                    "Return types differ: old {:?}, new {:?}",
                    old_results, new_results
                ),
            });
        }

        Ok(())
    }

    fn validate_memory_types(old: &MemoryType, new: &MemoryType) -> Result<(), ValidationError> {
        if new.minimum() < old.minimum() {
            return Err(ValidationError::MemorySizeMismatch {
                expected: old.minimum(),
                got: new.minimum(),
            });
        }

        Ok(())
    }

    fn validate_table_types(_old: &TableType, _new: &TableType) -> Result<(), ValidationError> {
        Ok(())
    }

    fn validate_global_types(_old: &GlobalType, _new: &GlobalType) -> Result<(), ValidationError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError::MissingExport("test_func".to_string());
        assert_eq!(err.to_string(), "Missing export: 'test_func'");

        let err = ValidationError::IncompatibleSignature {
            function: "add".to_string(),
            details: "Parameter count mismatch".to_string(),
        };
        assert!(err.to_string().contains("Incompatible signature"));
    }
}
