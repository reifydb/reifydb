//! Virtual table definitions for FFI operators

use core::ffi::{c_char, c_void};
use crate::types::*;

/// Virtual function table for FFI operators
///
/// This unified interface provides all methods an operator might need.
/// Operators that don't use certain features (e.g., state) simply won't
/// call those methods. All function pointers must be valid (non-null).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FFIOperatorVTable {
    // ==================== Core Methods (Required) ====================

    /// Apply the operator to a flow change
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `txn`: Transaction handle for this operation
    /// - `input`: Input flow change
    /// - `output`: Output flow change (to be filled by operator)
    ///
    /// # Returns
    /// - 0 on success, negative error code on failure
    pub apply: extern "C" fn(
        instance: *mut c_void,
        txn: *mut TransactionHandle,
        input: *const FlowChangeFFI,
        output: *mut FlowChangeFFI,
    ) -> i32,

    /// Get specific rows by their row numbers
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `txn`: Transaction handle for this operation
    /// - `row_numbers`: Array of row numbers to fetch
    /// - `count`: Number of row numbers
    /// - `output`: Output rows structure (to be filled)
    ///
    /// # Returns
    /// - 0 on success, negative error code on failure
    pub get_rows: extern "C" fn(
        instance: *mut c_void,
        txn: *mut TransactionHandle,
        row_numbers: *const u64,
        count: usize,
        output: *mut RowsFFI,
    ) -> i32,

    /// Destroy the operator instance and free its resources
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer to destroy
    pub destroy: extern "C" fn(instance: *mut c_void),

    // ==================== State Management Methods ====================
    // These methods are always present in the vtable. Operators that don't
    // use state simply won't call them. The host provides implementations
    // that handle state operations.

    /// Get a value from operator state
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `txn`: Transaction handle for this operation
    /// - `key`: Key bytes
    /// - `key_len`: Length of key
    /// - `output`: Output buffer for value (to be filled)
    ///
    /// # Returns
    /// - 0 on success, -6 (NotFound) if key doesn't exist, other negative on error
    pub state_get: extern "C" fn(
        instance: *mut c_void,
        txn: *mut TransactionHandle,
        key: *const u8,
        key_len: usize,
        output: *mut BufferFFI,
    ) -> i32,

    /// Set a value in operator state
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `txn`: Transaction handle for this operation
    /// - `key`: Key bytes
    /// - `key_len`: Length of key
    /// - `value`: Value bytes
    /// - `value_len`: Length of value
    ///
    /// # Returns
    /// - 0 on success, negative error code on failure
    pub state_set: extern "C" fn(
        instance: *mut c_void,
        txn: *mut TransactionHandle,
        key: *const u8,
        key_len: usize,
        value: *const u8,
        value_len: usize,
    ) -> i32,

    /// Remove a value from operator state
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `txn`: Transaction handle for this operation
    /// - `key`: Key bytes
    /// - `key_len`: Length of key
    ///
    /// # Returns
    /// - 0 on success, negative error code on failure
    pub state_remove: extern "C" fn(
        instance: *mut c_void,
        txn: *mut TransactionHandle,
        key: *const u8,
        key_len: usize,
    ) -> i32,

    /// Scan all state entries
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `txn`: Transaction handle for this operation
    /// - `iterator_out`: Output iterator handle (to be filled)
    ///
    /// # Returns
    /// - 0 on success, negative error code on failure
    pub state_scan: extern "C" fn(
        instance: *mut c_void,
        txn: *mut TransactionHandle,
        iterator_out: *mut *mut StateIteratorFFI,
    ) -> i32,

    /// Get state entries in a key range
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `txn`: Transaction handle for this operation
    /// - `start_key`: Start of key range (inclusive)
    /// - `start_len`: Length of start key
    /// - `end_key`: End of key range (exclusive)
    /// - `end_len`: Length of end key
    /// - `iterator_out`: Output iterator handle (to be filled)
    ///
    /// # Returns
    /// - 0 on success, negative error code on failure
    pub state_range: extern "C" fn(
        instance: *mut c_void,
        txn: *mut TransactionHandle,
        start_key: *const u8,
        start_len: usize,
        end_key: *const u8,
        end_len: usize,
        iterator_out: *mut *mut StateIteratorFFI,
    ) -> i32,

    /// Clear all state for this operator
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `txn`: Transaction handle for this operation
    ///
    /// # Returns
    /// - 0 on success, negative error code on failure
    pub state_clear: extern "C" fn(
        instance: *mut c_void,
        txn: *mut TransactionHandle,
    ) -> i32,

    /// Encode multiple values into a state key
    ///
    /// This is useful for implementing KeyedStateful patterns where
    /// state is indexed by multiple field values.
    ///
    /// # Parameters
    /// - `instance`: The operator instance pointer
    /// - `values`: Array of values to encode
    /// - `value_count`: Number of values
    /// - `output`: Output buffer for encoded key (to be filled)
    ///
    /// # Returns
    /// - 0 on success, negative error code on failure
    pub state_encode_key: extern "C" fn(
        instance: *mut c_void,
        values: *const ValueFFI,
        value_count: usize,
        output: *mut BufferFFI,
    ) -> i32,
}

/// Descriptor for an FFI operator
///
/// This structure describes an operator's capabilities and provides
/// its virtual function table.
#[repr(C)]
pub struct FFIOperatorDescriptor {
    /// API version (must match CURRENT_API_VERSION)
    pub api_version: u32,

    /// Operator name (null-terminated C string)
    pub operator_name: *const c_char,

    /// Capability flags (informational, not restrictive)
    /// These help the host optimize and debug but don't limit functionality
    pub capabilities: u32,

    /// Virtual function table with all operator methods
    pub vtable: FFIOperatorVTable,
}

/// Factory function type for creating operator instances
pub type FFIOperatorCreateFn = extern "C" fn(
    config: *const u8,
    config_len: usize,
) -> *mut c_void;