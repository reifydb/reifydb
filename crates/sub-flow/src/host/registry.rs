//! Registry for FFI operators

use crate::host::FFIOperatorLoader;
use std::collections::HashSet;

/// Registry for FFI operators
pub struct FFIOperatorRegistry {
    loader: FFIOperatorLoader,
    registered_types: HashSet<String>,
}

impl FFIOperatorRegistry {
    /// Create a new FFI operator registry
    pub fn new(loader: FFIOperatorLoader) -> Self {
        Self {
            loader,
            registered_types: HashSet::new(),
        }
    }

    /// Register an operator type
    pub fn register_type(&mut self, operator_type: String) {
        self.registered_types.insert(operator_type);
    }

    /// Get the loader
    pub fn loader(&self) -> &FFIOperatorLoader {
        &self.loader
    }

    /// Get the registered types
    pub fn registered_types(&self) -> &HashSet<String> {
        &self.registered_types
    }
}