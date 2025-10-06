use serde::{Deserialize, Serialize};
use std::fmt;

/// Global process identifier that provides location transparency across distributed nodes
///
/// Format: (node_id, environment_id, process_id)
/// - node_id: Identifies which node the process is running on
/// - environment_id: Identifies the environment within the node
/// - process_id: Local process ID within the environment
///
/// This enables transparent process addressing: send messages to any process
/// regardless of which node it's running on, matching Erlang's distributed semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalProcessId {
    node_id: u64,
    environment_id: u64,
    process_id: u64,
}

impl GlobalProcessId {
    /// Create a new global process ID
    pub fn new(node_id: u64, environment_id: u64, process_id: u64) -> Self {
        Self {
            node_id,
            environment_id,
            process_id,
        }
    }

    /// Get the node ID where this process is located
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get the environment ID
    pub fn environment_id(&self) -> u64 {
        self.environment_id
    }

    /// Get the local process ID
    pub fn process_id(&self) -> u64 {
        self.process_id
    }

    /// Check if this process is local to the given node
    pub fn is_local(&self, current_node_id: u64) -> bool {
        self.node_id == current_node_id
    }

    /// Encode as a compact u128 for efficient storage
    /// Format: [node_id:40 | env_id:40 | proc_id:48]
    pub fn to_compact(&self) -> u128 {
        ((self.node_id as u128) << 88)
            | ((self.environment_id as u128) << 48)
            | (self.process_id as u128)
    }

    /// Decode from compact u128 format
    pub fn from_compact(compact: u128) -> Self {
        Self {
            node_id: ((compact >> 88) & 0xFF_FFFF_FFFF) as u64,
            environment_id: ((compact >> 48) & 0xFF_FFFF_FFFF) as u64,
            process_id: (compact & 0xFFFF_FFFF_FFFF) as u64,
        }
    }
}

impl fmt::Display for GlobalProcessId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GlobalPid(node:{}, env:{}, pid:{})",
            self.node_id, self.environment_id, self.process_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_process_id_creation() {
        let gpid = GlobalProcessId::new(1, 2, 3);
        assert_eq!(gpid.node_id(), 1);
        assert_eq!(gpid.environment_id(), 2);
        assert_eq!(gpid.process_id(), 3);
    }

    #[test]
    fn test_is_local() {
        let gpid = GlobalProcessId::new(5, 1, 100);
        assert!(gpid.is_local(5));
        assert!(!gpid.is_local(6));
    }

    #[test]
    fn test_compact_encoding() {
        let original = GlobalProcessId::new(0xFF_FFFF, 0xAA_BBBB, 0x1234_5678_9ABC);
        let compact = original.to_compact();
        let decoded = GlobalProcessId::from_compact(compact);

        assert_eq!(original, decoded);
        assert_eq!(original.node_id(), decoded.node_id());
        assert_eq!(original.environment_id(), decoded.environment_id());
        assert_eq!(original.process_id(), decoded.process_id());
    }

    #[test]
    fn test_display_format() {
        let gpid = GlobalProcessId::new(1, 2, 3);
        let display = format!("{}", gpid);
        assert_eq!(display, "GlobalPid(node:1, env:2, pid:3)");
    }

    #[test]
    fn test_hash_and_eq() {
        use std::collections::HashSet;

        let gpid1 = GlobalProcessId::new(1, 2, 3);
        let gpid2 = GlobalProcessId::new(1, 2, 3);
        let gpid3 = GlobalProcessId::new(1, 2, 4);

        assert_eq!(gpid1, gpid2);
        assert_ne!(gpid1, gpid3);

        let mut set = HashSet::new();
        set.insert(gpid1);
        assert!(set.contains(&gpid2));
        assert!(!set.contains(&gpid3));
    }
}
