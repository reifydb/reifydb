//! FFI operator implementation that bridges FFI operators with ReifyDB

use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice::from_raw_parts;
use std::sync::Mutex;

use reifydb_core::{interface::FlowNodeId, Row};
use reifydb_engine::StandardRowEvaluator;
use reifydb_operator_abi::{FFIOperatorDescriptor, FFIOperatorVTable};
use reifydb_type::RowNumber;

use crate::flow::FlowChange;
use crate::host::{create_host_callbacks, FFIMarshaller, TransactionHandle};
use crate::operator::Operator;
use crate::transaction::FlowTransaction;
use crate::Result;

/// FFI operator that wraps an external operator implementation
pub struct FFIOperator {
	/// Operator descriptor from the FFI library
	descriptor: FFIOperatorDescriptor,

	/// Virtual function table for calling FFI functions
	vtable: FFIOperatorVTable,

	/// Pointer to the FFI operator instance
	instance: *mut c_void,

	/// Node ID for this operator
	node_id: FlowNodeId,

	/// Marshaller for type conversions (protected by mutex for thread safety)
	marshaller: Mutex<FFIMarshaller>,
}

// SAFETY: FFIOperator manages an FFI pointer but ensures proper synchronization
unsafe impl Send for FFIOperator {}
unsafe impl Sync for FFIOperator {}

impl FFIOperator {
	/// Create a new FFI operator
	pub fn new(descriptor: FFIOperatorDescriptor, instance: *mut c_void, node_id: FlowNodeId) -> Self {
		let vtable = descriptor.vtable;

		Self {
			descriptor,
			vtable,
			instance,
			node_id,
			marshaller: Mutex::new(FFIMarshaller::new()),
		}
	}

	/// Check if this operator uses state
	pub fn is_stateful(&self) -> bool {
		self.descriptor.capabilities & reifydb_operator_abi::CAP_USES_STATE != 0
	}

	/// Check if this operator uses keyed state
	pub fn is_keyed(&self) -> bool {
		self.descriptor.capabilities & reifydb_operator_abi::CAP_KEYED_STATE != 0
	}
}

impl Operator for FFIOperator {
	fn id(&self) -> FlowNodeId {
		self.node_id
	}

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> Result<FlowChange> {
		// Lock the marshaller for this operation
		let mut marshaller = self.marshaller.lock().unwrap();

		// Marshal the input change
		let ffi_input = marshaller.marshal_flow_change(&change);

		// Create output holder
		let mut ffi_output = reifydb_operator_abi::FlowChangeFFI::empty();

		// Create transaction handle
		let txn_handle = TransactionHandle::new(txn, self.node_id, create_host_callbacks());
		let txn_handle_ptr = &txn_handle as *const _ as *mut reifydb_operator_abi::TransactionHandle;

		// Call FFI apply function
		let result = unsafe {
			catch_unwind(AssertUnwindSafe(|| {
				(self.vtable.apply)(self.instance, txn_handle_ptr, &ffi_input, &mut ffi_output)
			}))
		};

		// Handle panics from FFI code
		let result_code = match result {
			Ok(code) => code,
			Err(_) => {
				return Err(crate::host::FFIError::Other(
					"FFI operator panicked during apply".to_string(),
				)
				.into());
			}
		};

		// Check result code
		if result_code != 0 {
			return Err(crate::host::FFIError::Other(format!(
				"FFI operator apply failed with code: {}",
				result_code
			))
			.into());
		}

		// Unmarshal the output
		marshaller.unmarshal_flow_change(&ffi_output)
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Vec<Option<Row>>> {
		// Lock the marshaller for this operation
		let mut marshaller = self.marshaller.lock().unwrap();

		// Convert row numbers to u64 array
		let row_numbers: Vec<u64> = rows.iter().map(|r| (*r).into()).collect();

		// Create output holder
		let mut ffi_output = reifydb_operator_abi::RowsFFI {
			count: 0,
			rows: std::ptr::null_mut(),
		};

		// Create transaction handle
		let txn_handle = TransactionHandle::new(txn, self.node_id, create_host_callbacks());
		let txn_handle_ptr = &txn_handle as *const _ as *mut reifydb_operator_abi::TransactionHandle;

		// Call FFI get_rows function
		let result = catch_unwind(AssertUnwindSafe(|| {
			(self.vtable.get_rows)(
				self.instance,
				txn_handle_ptr,
				row_numbers.as_ptr(),
				row_numbers.len(),
				&mut ffi_output,
			)
		}));

		// Handle panics from FFI code
		let result_code = match result {
			Ok(code) => code,
			Err(_) => {
				return Err(crate::host::FFIError::Other(
					"FFI operator panicked during get_rows".to_string(),
				)
				.into());
			}
		};

		// Check result code
		if result_code != 0 {
			return Err(crate::host::FFIError::Other(format!(
				"FFI operator get_rows failed with code: {}",
				result_code
			))
			.into());
		}

		// Unmarshal the rows
		let mut result_rows = Vec::with_capacity(ffi_output.count);

		if !ffi_output.rows.is_null() && ffi_output.count > 0 {
			unsafe {
				let rows_array = from_raw_parts(ffi_output.rows, ffi_output.count);

				for &row_ptr in rows_array {
					if row_ptr.is_null() {
						result_rows.push(None);
					} else {
						let row = marshaller.unmarshal_row(&*row_ptr);
						result_rows.push(Some(row));
					}
				}
			}
		}

		// Clear the marshaller's arena after operation
		marshaller.clear();

		Ok(result_rows)
	}
}

impl Drop for FFIOperator {
	fn drop(&mut self) {
		if !self.instance.is_null() {
			let _ = catch_unwind(AssertUnwindSafe(|| {
				(self.vtable.destroy)(self.instance);
			}));
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_ffi_operator_capabilities() {
		// Create a mock descriptor
		let descriptor = FFIOperatorDescriptor {
			api_version: 1,
			operator_name: std::ptr::null(),
			capabilities: reifydb_operator_abi::CAP_USES_STATE | reifydb_operator_abi::CAP_KEYED_STATE,
			vtable: unsafe { std::mem::zeroed() }, // Don't use in real code
		};

		let operator = FFIOperator::new(descriptor, std::ptr::null_mut(), FlowNodeId(42));

		assert_eq!(operator.id(), FlowNodeId(42));
		assert!(operator.is_stateful());
		assert!(operator.is_keyed());
	}
}
