//! FFI operator loader for loading shared libraries

use libloading::Library;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use reifydb_core::interface::FlowNodeId;

/// Loads and manages FFI operator libraries
pub struct FFIOperatorLoader {
    loaded_libraries: HashMap<PathBuf, Library>,
    // TODO: Add operator factories
}

impl FFIOperatorLoader {
    /// Create a new FFI operator loader
    pub fn new() -> Self {
        Self {
            loaded_libraries: HashMap::new(),
        }
    }

    /// Load an operator from a shared library
    pub fn load_operator(&mut self, path: &Path) -> Result<(), crate::host::FFIError> {
        Ok(())
    }

    /// Create an operator instance
    pub fn create_operator(
        &self,
        _operator_type: &str,
        _operator_id: FlowNodeId,
        _config: &[u8],
    ) -> Result<Box<dyn crate::Operator>, crate::host::FFIError> {
        // TODO: Implement operator creation
        Err(crate::host::FFIError::NotSupported)
    }
}

impl Default for FFIOperatorLoader {
    fn default() -> Self {
        Self::new()
    }
}