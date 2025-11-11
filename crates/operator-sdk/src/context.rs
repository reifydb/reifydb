//! Operator context providing access to state and resources

use crate::error::{Error, Result};
use crate::state::State;
use reifydb_core::interface::FlowNodeId;
use reifydb_operator_abi::{TransactionHandle, BufferFFI, StateIteratorFFI};

// Extern functions provided by the host for state operations
unsafe extern "C" {
    /// Get state value for a key
    fn host_state_get(
        node_id: u64,
        txn: *mut TransactionHandle,
        key: *const u8,
        key_len: usize,
        output: *mut BufferFFI,
    ) -> i32;

    /// Set state value for a key
    fn host_state_set(
        node_id: u64,
        txn: *mut TransactionHandle,
        key: *const u8,
        key_len: usize,
        value: *const u8,
        value_len: usize,
    ) -> i32;

    /// Remove state value for a key
    fn host_state_remove(
        node_id: u64,
        txn: *mut TransactionHandle,
        key: *const u8,
        key_len: usize,
    ) -> i32;

    /// Scan state entries with a prefix
    fn host_state_prefix(
        node_id: u64,
        txn: *mut TransactionHandle,
        prefix: *const u8,
        prefix_len: usize,
        iterator_out: *mut *mut StateIteratorFFI,
    ) -> i32;

    /// Clear all state for the node
    fn host_state_clear(
        node_id: u64,
        txn: *mut TransactionHandle,
    ) -> i32;

    /// Get next item from state iterator
    fn host_state_iterator_next(
        iterator: *mut StateIteratorFFI,
        key_out: *mut BufferFFI,
        value_out: *mut BufferFFI,
    ) -> i32;

    /// Free state iterator
    fn host_state_iterator_free(
        iterator: *mut StateIteratorFFI,
    );
}

/// Operator context providing access to state and other resources
pub struct OperatorContext {
    /// ID for this operator
    operator_id: FlowNodeId,
    /// FFI transaction handle for state operations
    tx_handle: *mut TransactionHandle,
}


impl OperatorContext {
    /// Create a new operator context with transaction handle
    pub fn new(node_id: FlowNodeId, tx_handle: *mut TransactionHandle) -> Self {
        Self {
            operator_id: node_id,
            tx_handle,
        }
    }

    /// Get the node ID
    pub fn node_id(&self) -> FlowNodeId {
        self.operator_id
    }

    /// Get a state manager
    pub fn state(&mut self) -> State<'_> {
        State::new(self)
    }

    // Internal state methods used by State
    pub(crate) fn raw_state_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let key_bytes = key.as_bytes();
        let mut output = BufferFFI {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        };

        unsafe {
            let result = host_state_get(
                self.operator_id.0,
                self.tx_handle,
                key_bytes.as_ptr(),
                key_bytes.len(),
                &mut output,
            );

            if result == 0 {
                // Success - value found
                if output.ptr.is_null() || output.len == 0 {
                    Ok(None)
                } else {
                    let value = std::slice::from_raw_parts(output.ptr, output.len).to_vec();
                    // TODO: Free the buffer using host dealloc
                    Ok(Some(value))
                }
            } else if result == -6 {
                // NotFound error code
                Ok(None)
            } else {
                Err(Error::FFI(format!("host_state_get failed with code {}", result)))
            }
        }
    }

    pub(crate) fn raw_state_set(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let key_bytes = key.as_bytes();

        unsafe {
            let result = host_state_set(
                self.operator_id.0,
                self.tx_handle,
                key_bytes.as_ptr(),
                key_bytes.len(),
                value.as_ptr(),
                value.len(),
            );

            if result == 0 {
                Ok(())
            } else {
                Err(Error::FFI(format!("host_state_set failed with code {}", result)))
            }
        }
    }

    pub(crate) fn raw_state_remove(&mut self, key: &str) -> Result<()> {
        let key_bytes = key.as_bytes();

        unsafe {
            let result = host_state_remove(
                self.operator_id.0,
                self.tx_handle,
                key_bytes.as_ptr(),
                key_bytes.len(),
            );

            if result == 0 {
                Ok(())
            } else {
                Err(Error::FFI(format!("host_state_remove failed with code {}", result)))
            }
        }
    }

    pub(crate) fn raw_state_scan(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let prefix_bytes = prefix.as_bytes();
        let mut iterator: *mut StateIteratorFFI = std::ptr::null_mut();

        unsafe {
            let result = host_state_prefix(
                self.operator_id.0,
                self.tx_handle,
                prefix_bytes.as_ptr(),
                prefix_bytes.len(),
                &mut iterator,
            );

            if result != 0 {
                return Err(Error::FFI(format!("host_state_prefix failed with code {}", result)));
            }

            if iterator.is_null() {
                return Ok(Vec::new());
            }

            let mut results = Vec::new();

            loop {
                let mut key_buf = BufferFFI {
                    ptr: std::ptr::null_mut(),
                    len: 0,
                    cap: 0,
                };
                let mut value_buf = BufferFFI {
                    ptr: std::ptr::null_mut(),
                    len: 0,
                    cap: 0,
                };

                let next_result = host_state_iterator_next(
                    iterator,
                    &mut key_buf,
                    &mut value_buf,
                );

                if next_result == 1 {
                    // End of iteration
                    break;
                } else if next_result != 0 {
                    host_state_iterator_free(iterator);
                    return Err(Error::FFI(format!("host_state_iterator_next failed with code {}", next_result)));
                }

                // Convert buffers to owned data
                if !key_buf.ptr.is_null() && key_buf.len > 0 {
                    let key_slice = std::slice::from_raw_parts(key_buf.ptr, key_buf.len);
                    let key = String::from_utf8_lossy(key_slice).to_string();

                    let value = if !value_buf.ptr.is_null() && value_buf.len > 0 {
                        std::slice::from_raw_parts(value_buf.ptr, value_buf.len).to_vec()
                    } else {
                        Vec::new()
                    };

                    results.push((key, value));
                    // TODO: Free key_buf and value_buf using host dealloc
                }
            }

            host_state_iterator_free(iterator);
            Ok(results)
        }
    }

    pub(crate) fn raw_state_clear(&mut self) -> Result<()> {
        unsafe {
            let result = host_state_clear(
                self.operator_id.0,
                self.tx_handle,
            );

            if result == 0 {
                Ok(())
            } else {
                Err(Error::FFI(format!("host_state_clear failed with code {}", result)))
            }
        }
    }
}