use serde::{de::DeserializeOwned, Serialize};

// One unit of fuel represents around 100k instructions.
pub const UNIT_OF_COMPUTE_IN_INSTRUCTIONS: u64 = 100_000;

/// Common process configuration.
///
/// Each process in lunatic can have specific limits and permissions. These properties are set
/// through a process configuration that is used when a process is spawned. Once the process is
/// spawned the configuration can't be changed anymore. The process configuration heavily depends
/// on the [`ProcessState`](crate::state::ProcessState) that defines host functions available to
/// the process. This host functions are the ones that consider specific configuration while
/// performing operations.
///
/// However, two properties of a process are enforced by the runtime (maximum memory and maximum
/// fuel usage). This two properties need to be part of every configuration.
///
/// `ProcessConfig` must be serializable in case it is used to spawn processes on other nodes.
pub trait ProcessConfig: Clone + Serialize + DeserializeOwned {
    fn set_max_fuel(&mut self, max_fuel: Option<u64>);
    fn get_max_fuel(&self) -> Option<u64>;
    fn set_max_memory(&mut self, max_memory: usize);
    fn get_max_memory(&self) -> usize;

    /// Creates a deny-by-default child configuration whose resource ceilings
    /// do not exceed this configuration.
    ///
    /// The fail-closed default keeps existing custom `ProcessConfig`
    /// implementations source-compatible while requiring them to opt in to a
    /// concrete attenuation policy before guest config creation is enabled.
    fn new_child_config(&self) -> Result<Self, String> {
        Err("process config does not define a child attenuation policy".into())
    }

    /// Verifies that `child` delegates no capability or resource authority
    /// that this configuration does not possess.
    fn validate_child_config(&self, _child: &Self) -> Result<(), String> {
        Err("process config does not define a child attenuation policy".into())
    }

    /// Verifies that this configuration contains no intrinsically host-local
    /// authority that is unsafe to interpret on another host.
    ///
    /// Host-local authorities, such as filesystem paths, must be rejected
    /// unless the receiving node has an explicit policy for translating and
    /// authorizing them. This check does not authenticate the sender or apply
    /// receiver-owned ceilings to otherwise portable fields.
    fn validate_distributed_config(&self) -> Result<(), String> {
        Err("process config does not define a distributed authority policy".into())
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::ProcessConfig;

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct PreAttenuationConfig {
        max_fuel: Option<u64>,
        max_memory: usize,
    }

    // This intentionally implements only the pre-attenuation required methods.
    impl ProcessConfig for PreAttenuationConfig {
        fn set_max_fuel(&mut self, max_fuel: Option<u64>) {
            self.max_fuel = max_fuel;
        }

        fn get_max_fuel(&self) -> Option<u64> {
            self.max_fuel
        }

        fn set_max_memory(&mut self, max_memory: usize) {
            self.max_memory = max_memory;
        }

        fn get_max_memory(&self) -> usize {
            self.max_memory
        }
    }

    #[test]
    fn existing_custom_configs_compile_and_fail_closed() {
        let config = PreAttenuationConfig::default();
        assert!(config.new_child_config().is_err());
        assert!(config.validate_child_config(&config).is_err());
        assert!(config.validate_distributed_config().is_err());
    }
}
