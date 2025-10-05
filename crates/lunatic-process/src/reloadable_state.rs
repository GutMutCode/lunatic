use anyhow::Result;

/// Trait for process state that can be preserved across hot reloads.
/// 
/// This trait enables Erlang-style hot code reloading by allowing process state
/// to be serialized before a reload and deserialized into the new code version.
/// 
/// # Example
/// 
/// ```rust
/// use lunatic_process::reloadable_state::ReloadableState;
/// use anyhow::Result;
/// 
/// struct Counter {
///     value: i32,
/// }
/// 
/// impl ReloadableState for Counter {
///     fn serialize_state(&self) -> Result<Vec<u8>> {
///         Ok(self.value.to_le_bytes().to_vec())
///     }
///     
///     fn deserialize_state(bytes: &[u8]) -> Result<Self> {
///         let value = i32::from_le_bytes(bytes.try_into()?);
///         Ok(Counter { value })
///     }
/// }
/// ```
pub trait ReloadableState: Sized {
    /// Serialize the current process state to bytes.
    /// 
    /// This method is called before a hot reload to capture the process state.
    /// The serialized bytes will be passed to `deserialize_state` in the new code version.
    /// 
    /// # Errors
    /// 
    /// Returns an error if the state cannot be serialized (e.g., contains non-serializable data).
    fn serialize_state(&self) -> Result<Vec<u8>>;
    
    /// Deserialize process state from bytes.
    /// 
    /// This method is called after a hot reload to restore the process state
    /// from the previous code version.
    /// 
    /// # Errors
    /// 
    /// Returns an error if the bytes cannot be deserialized into valid state
    /// (e.g., corrupted data, version mismatch).
    fn deserialize_state(bytes: &[u8]) -> Result<Self>;
    
    /// Optional callback for transforming state between code versions.
    /// 
    /// This is similar to Erlang's `code_change/3` callback. It allows you to
    /// migrate state when the structure changes between versions.
    /// 
    /// # Arguments
    /// 
    /// * `old_version` - Version number of the previous code
    /// * `new_version` - Version number of the new code
    /// 
    /// # Default Implementation
    /// 
    /// The default implementation does nothing, assuming state structure is compatible.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// # use lunatic_process::reloadable_state::ReloadableState;
    /// # use anyhow::Result;
    /// struct Counter {
    ///     value: i32,
    ///     // Added in v2
    ///     multiplier: i32,
    /// }
    /// 
    /// impl ReloadableState for Counter {
    ///     // ... serialize/deserialize implementations ...
    ///     # fn serialize_state(&self) -> Result<Vec<u8>> { Ok(vec![]) }
    ///     # fn deserialize_state(bytes: &[u8]) -> Result<Self> {
    ///     #     Ok(Counter { value: 0, multiplier: 1 })
    ///     # }
    ///     
    ///     fn code_change(&mut self, old_version: u32, new_version: u32) -> Result<()> {
    ///         if old_version == 1 && new_version == 2 {
    ///             // Initialize new field for v1 -> v2 migration
    ///             self.multiplier = 1;
    ///         }
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn code_change(&mut self, _old_version: u32, _new_version: u32) -> Result<()> {
        Ok(())
    }
}

/// Implementation for unit type (stateless processes)
impl ReloadableState for () {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    
    fn deserialize_state(_bytes: &[u8]) -> Result<Self> {
        Ok(())
    }
}

/// Implementation for integers
impl ReloadableState for i32 {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 4] = bytes.try_into()
            .map_err(|_| anyhow::anyhow!("Invalid i32 bytes"))?;
        Ok(i32::from_le_bytes(arr))
    }
}

impl ReloadableState for i64 {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = bytes.try_into()
            .map_err(|_| anyhow::anyhow!("Invalid i64 bytes"))?;
        Ok(i64::from_le_bytes(arr))
    }
}

impl ReloadableState for u32 {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 4] = bytes.try_into()
            .map_err(|_| anyhow::anyhow!("Invalid u32 bytes"))?;
        Ok(u32::from_le_bytes(arr))
    }
}

impl ReloadableState for u64 {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = bytes.try_into()
            .map_err(|_| anyhow::anyhow!("Invalid u64 bytes"))?;
        Ok(u64::from_le_bytes(arr))
    }
}

/// Implementation for String
impl ReloadableState for String {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| anyhow::anyhow!("Invalid UTF-8: {}", e))
    }
}

/// Implementation for Vec<T> where T: ReloadableState
impl<T: ReloadableState> ReloadableState for Vec<T> {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        
        // Store length
        result.extend_from_slice(&(self.len() as u64).to_le_bytes());
        
        // Store each element with its length prefix
        for item in self {
            let item_bytes = item.serialize_state()?;
            result.extend_from_slice(&(item_bytes.len() as u64).to_le_bytes());
            result.extend_from_slice(&item_bytes);
        }
        
        Ok(result)
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 {
            return Err(anyhow::anyhow!("Invalid Vec bytes: too short"));
        }
        
        let len = u64::from_le_bytes(bytes[0..8].try_into()?) as usize;
        let mut result = Vec::with_capacity(len);
        let mut offset = 8;
        
        for _ in 0..len {
            if offset + 8 > bytes.len() {
                return Err(anyhow::anyhow!("Invalid Vec bytes: truncated"));
            }
            
            let item_len = u64::from_le_bytes(bytes[offset..offset+8].try_into()?) as usize;
            offset += 8;
            
            if offset + item_len > bytes.len() {
                return Err(anyhow::anyhow!("Invalid Vec bytes: item truncated"));
            }
            
            let item = T::deserialize_state(&bytes[offset..offset+item_len])?;
            result.push(item);
            offset += item_len;
        }
        
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unit_roundtrip() {
        let state = ();
        let bytes = state.serialize_state().unwrap();
        let restored = <()>::deserialize_state(&bytes).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn test_i32_roundtrip() {
        let state = 42i32;
        let bytes = state.serialize_state().unwrap();
        let restored = i32::deserialize_state(&bytes).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn test_string_roundtrip() {
        let state = String::from("Hello, hot reload!");
        let bytes = state.serialize_state().unwrap();
        let restored = String::deserialize_state(&bytes).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn test_vec_i32_roundtrip() {
        let state = vec![1, 2, 3, 42, 100];
        let bytes = state.serialize_state().unwrap();
        let restored = Vec::<i32>::deserialize_state(&bytes).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn test_vec_string_roundtrip() {
        let state = vec![
            String::from("hello"),
            String::from("world"),
            String::from("hot reload"),
        ];
        let bytes = state.serialize_state().unwrap();
        let restored = Vec::<String>::deserialize_state(&bytes).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn test_code_change_default() {
        let mut state = 42i32;
        let result = state.code_change(1, 2);
        assert!(result.is_ok());
        assert_eq!(state, 42);
    }
}
