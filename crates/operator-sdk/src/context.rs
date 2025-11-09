//! Operator context providing access to state and resources

use crate::error::{Error, Result};
use crate::state::State;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;

/// Operator context providing access to state and other resources
pub struct OperatorContext {
    /// Node ID for this operator
    node_id: reifydb_core::interface::FlowNodeId,

    /// In-memory state storage (for testing and simple use cases)
    state_storage: HashMap<String, Vec<u8>>,

    /// FFI transaction handle (when running as FFI operator)
    ffi_handle: Option<*mut reifydb_operator_api::TransactionHandle>,
}

impl OperatorContext {
    /// Create a new operator context
    pub fn new(node_id: reifydb_core::interface::FlowNodeId) -> Self {
        Self {
            node_id,
            state_storage: HashMap::new(),
            ffi_handle: None,
        }
    }

    /// Create a context with FFI handle
    pub fn with_ffi_handle(
        node_id: reifydb_core::interface::FlowNodeId,
        handle: *mut reifydb_operator_api::TransactionHandle,
    ) -> Self {
        Self {
            node_id,
            state_storage: HashMap::new(),
            ffi_handle: Some(handle),
        }
    }

    /// Get the node ID
    pub fn node_id(&self) -> reifydb_core::interface::FlowNodeId {
        self.node_id
    }

    /// Get a state manager
    pub fn state(&mut self) -> State<'_> {
        State::new(self)
    }

    // Internal state methods used by State

    pub(crate) fn raw_state_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if let Some(_handle) = self.ffi_handle {
            // TODO: Call FFI state_get when we have proper FFI integration
            Err(Error::NotImplemented("FFI state access not yet implemented".to_string()))
        } else {
            // Use in-memory storage for testing
            Ok(self.state_storage.get(key).cloned())
        }
    }

    pub(crate) fn raw_state_set(&mut self, key: &str, value: &[u8]) -> Result<()> {
        if let Some(_handle) = self.ffi_handle {
            // TODO: Call FFI state_set when we have proper FFI integration
            Err(Error::NotImplemented("FFI state access not yet implemented".to_string()))
        } else {
            // Use in-memory storage for testing
            self.state_storage.insert(key.to_string(), value.to_vec());
            Ok(())
        }
    }

    pub(crate) fn raw_state_remove(&mut self, key: &str) -> Result<()> {
        if let Some(_handle) = self.ffi_handle {
            // TODO: Call FFI state_remove when we have proper FFI integration
            Err(Error::NotImplemented("FFI state access not yet implemented".to_string()))
        } else {
            // Use in-memory storage for testing
            self.state_storage.remove(key);
            Ok(())
        }
    }

    pub(crate) fn raw_state_scan(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        if let Some(_handle) = self.ffi_handle {
            // TODO: Call FFI state_scan when we have proper FFI integration
            Err(Error::NotImplemented("FFI state scan not yet implemented".to_string()))
        } else {
            // Use in-memory storage for testing
            let results: Vec<_> = self.state_storage
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            Ok(results)
        }
    }

    pub(crate) fn raw_state_clear(&mut self) -> Result<()> {
        if let Some(_handle) = self.ffi_handle {
            // TODO: Call FFI state_clear when we have proper FFI integration
            Err(Error::NotImplemented("FFI state clear not yet implemented".to_string()))
        } else {
            // Use in-memory storage for testing
            self.state_storage.clear();
            Ok(())
        }
    }
}

/// Mock context for testing
pub struct MockContext {
    inner: OperatorContext,
}

impl MockContext {
    pub fn new() -> Self {
        Self {
            inner: OperatorContext::new(reifydb_core::interface::FlowNodeId(0)),
        }
    }

    pub fn with_node_id(node_id: reifydb_core::interface::FlowNodeId) -> Self {
        Self {
            inner: OperatorContext::new(node_id),
        }
    }

    pub fn as_mut(&mut self) -> &mut OperatorContext {
        &mut self.inner
    }

    /// Pre-populate state for testing
    pub fn with_state<T: Serialize>(mut self, key: &str, value: &T) -> Result<Self> {
        let compat = bincode::serde::Compat(value);
        let bytes = bincode::encode_to_vec(&compat, bincode::config::standard())?;
        self.inner.state_storage.insert(key.to_string(), bytes);
        Ok(self)
    }

    /// Check if state contains a key
    pub fn has_state(&self, key: &str) -> bool {
        self.inner.state_storage.contains_key(key)
    }

    /// Get state value for assertions
    pub fn get_state<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.inner.state_storage
            .get(key)
            .map(|bytes| {
                let compat_result: (bincode::serde::Compat<T>, _) =
                    bincode::decode_from_slice(bytes, bincode::config::standard())?;
                Ok::<T, crate::error::Error>(compat_result.0.0)
            })
            .transpose()
            .map_err(Into::into)
    }
}