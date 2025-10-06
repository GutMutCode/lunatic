pub mod client;
pub mod global_process_id;
pub mod message;
pub mod registry;
pub mod registry_coordination;
pub mod server;

pub use client::Client;
pub use global_process_id::GlobalProcessId;
pub use registry::{DistributedRegistry, ProcessName, RegistrationScope, RegistryEntry};
pub use registry_coordination::{
    GlobalRegisterResult, GlobalUnregisterResult, RegistryCoordinator,
    RegistryCoordinationMessage,
};
