//! Wrapper that bridges Rust operators to FFI interface

use crate::operator::{Operator, FlowChange, FlowDiff};
use crate::context::OperatorContext;
use reifydb_operator_api::*;
use reifydb_core::{Row, CowVec, interface::FlowNodeId, value::encoded::{EncodedValues, EncodedValuesNamedLayout}};
use reifydb_type::RowNumber;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

/// Wrapper that adapts a Rust operator to the FFI interface
pub struct OperatorWrapper<O: Operator> {
    operator: Mutex<O>,
    node_id: FlowNodeId,
}

impl<O: Operator> OperatorWrapper<O> {
    /// Create a new operator wrapper
    pub fn new(operator: O, node_id: FlowNodeId) -> Self {
        Self {
            operator: Mutex::new(operator),
            node_id,
        }
    }

    /// Get a pointer to this wrapper as c_void
    pub fn as_ptr(&mut self) -> *mut c_void {
        self as *mut _ as *mut c_void
    }

    /// Create from a raw pointer
    pub unsafe fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
        &mut *(ptr as *mut Self)
    }
}

// FFI callback implementations

pub extern "C" fn ffi_apply<O: Operator>(
    instance: *mut c_void,
    txn: *mut TransactionHandle,
    input: *const FlowChangeFFI,
    output: *mut FlowChangeFFI,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        unsafe {
            let wrapper = OperatorWrapper::<O>::from_ptr(instance);
            let mut operator = match wrapper.operator.lock() {
                Ok(op) => op,
                Err(_) => return -1,
            };

            // Convert FFI input to SDK types
            let input_change = unmarshal_flow_change(&*input);

            // Create context with FFI handle
            let mut ctx = OperatorContext::with_ffi_handle(wrapper.node_id, txn);

            // Call the operator
            let output_change = match operator.apply(&mut ctx, input_change) {
                Ok(change) => change,
                Err(_) => return -2,
            };

            // Marshal output
            *output = marshal_flow_change(&output_change);

            0 // Success
        }
    }));

    result.unwrap_or(-99)
}

pub extern "C" fn ffi_get_rows<O: Operator>(
    instance: *mut c_void,
    txn: *mut TransactionHandle,
    row_numbers: *const u64,
    count: usize,
    output: *mut RowsFFI,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        unsafe {
            let wrapper = OperatorWrapper::<O>::from_ptr(instance);
            let mut operator = match wrapper.operator.lock() {
                Ok(op) => op,
                Err(_) => return -1,
            };

            // Convert row numbers
            let numbers: Vec<RowNumber> = if !row_numbers.is_null() && count > 0 {
                std::slice::from_raw_parts(row_numbers, count)
                    .iter()
                    .map(|&n| RowNumber::from(n))
                    .collect()
            } else {
                Vec::new()
            };

            // Create context
            let mut ctx = OperatorContext::with_ffi_handle(wrapper.node_id, txn);

            // Call the operator
            let _rows = match operator.get_rows(&mut ctx, &numbers) {
                Ok(rows) => rows,
                Err(_) => return -2,
            };

            // Marshal output
            // For simplicity, we'll return empty rows for now
            // In a real implementation, we'd marshal the actual rows
            (*output).count = 0;
            (*output).rows = std::ptr::null_mut();

            0 // Success
        }
    }));

    result.unwrap_or(-99)
}

pub extern "C" fn ffi_destroy<O: Operator>(instance: *mut c_void) {
    unsafe {
        if !instance.is_null() {
            let wrapper = Box::from_raw(instance as *mut OperatorWrapper<O>);
            if let Ok(mut operator) = wrapper.operator.into_inner() {
                operator.destroy();
            }
        }
    }
}

// State method stubs (operators handle state through context)

pub extern "C" fn ffi_state_get(
    _instance: *mut c_void,
    _txn: *mut TransactionHandle,
    _key: *const u8,
    _key_len: usize,
    _output: *mut BufferFFI,
) -> i32 {
    -1 // Not directly exposed, use context.state()
}

pub extern "C" fn ffi_state_set(
    _instance: *mut c_void,
    _txn: *mut TransactionHandle,
    _key: *const u8,
    _key_len: usize,
    _value: *const u8,
    _value_len: usize,
) -> i32 {
    -1 // Not directly exposed, use context.state()
}

pub extern "C" fn ffi_state_remove(
    _instance: *mut c_void,
    _txn: *mut TransactionHandle,
    _key: *const u8,
    _key_len: usize,
) -> i32 {
    -1 // Not directly exposed, use context.state()
}

pub extern "C" fn ffi_state_scan(
    _instance: *mut c_void,
    _txn: *mut TransactionHandle,
    _iterator_out: *mut *mut StateIteratorFFI,
) -> i32 {
    -1 // Not directly exposed, use context.state()
}

pub extern "C" fn ffi_state_range(
    _instance: *mut c_void,
    _txn: *mut TransactionHandle,
    _start_key: *const u8,
    _start_len: usize,
    _end_key: *const u8,
    _end_len: usize,
    _iterator_out: *mut *mut StateIteratorFFI,
) -> i32 {
    -1 // Not directly exposed, use context.state()
}

pub extern "C" fn ffi_state_clear(
    _instance: *mut c_void,
    _txn: *mut TransactionHandle,
) -> i32 {
    -1 // Not directly exposed, use context.state()
}

pub extern "C" fn ffi_state_encode_key(
    _instance: *mut c_void,
    _values: *const ValueFFI,
    _value_count: usize,
    _output: *mut BufferFFI,
) -> i32 {
    -1 // Not directly exposed, use context.state()
}

// Marshalling helpers

fn unmarshal_flow_change(ffi: &FlowChangeFFI) -> FlowChange {
    let mut diffs = Vec::new();

    if !ffi.diffs.is_null() && ffi.diff_count > 0 {
        unsafe {
            let diffs_slice = std::slice::from_raw_parts(ffi.diffs, ffi.diff_count);
            for diff_ffi in diffs_slice {
                diffs.push(unmarshal_flow_diff(diff_ffi));
            }
        }
    }

    FlowChange {
        diffs,
        version: ffi.version,
    }
}

fn unmarshal_flow_diff(ffi: &FlowDiffFFI) -> FlowDiff {
    match ffi.diff_type {
        FlowDiffType::Insert => FlowDiff::Insert {
            post: unsafe { unmarshal_row(&*ffi.post_row) },
        },
        FlowDiffType::Update => FlowDiff::Update {
            pre: unsafe { unmarshal_row(&*ffi.pre_row) },
            post: unsafe { unmarshal_row(&*ffi.post_row) },
        },
        FlowDiffType::Remove => FlowDiff::Remove {
            pre: unsafe { unmarshal_row(&*ffi.pre_row) },
        },
    }
}

unsafe fn unmarshal_row(ffi: &RowFFI) -> Row {
    let encoded = if !ffi.encoded.ptr.is_null() && ffi.encoded.len > 0 {
        let slice = std::slice::from_raw_parts(ffi.encoded.ptr, ffi.encoded.len);
        EncodedValues(CowVec::new(slice.to_vec()))
    } else {
        EncodedValues(CowVec::new(Vec::new()))
    };

    Row {
        number: RowNumber::from(ffi.number),
        encoded,
        layout: EncodedValuesNamedLayout::new(std::iter::empty()), // Simplified
    }
}

fn marshal_flow_change(change: &FlowChange) -> FlowChangeFFI {
    // For now, return an empty change
    // In a real implementation, we'd allocate memory and marshal properly
    FlowChangeFFI {
        diff_count: 0,
        diffs: std::ptr::null_mut(),
        version: change.version,
    }
}

/// Create the vtable for an operator type
pub fn create_vtable<O: Operator>() -> FFIOperatorVTable {
    FFIOperatorVTable {
        apply: ffi_apply::<O>,
        get_rows: ffi_get_rows::<O>,
        destroy: ffi_destroy::<O>,
        state_get: ffi_state_get,
        state_set: ffi_state_set,
        state_remove: ffi_state_remove,
        state_scan: ffi_state_scan,
        state_range: ffi_state_range,
        state_clear: ffi_state_clear,
        state_encode_key: ffi_state_encode_key,
    }
}