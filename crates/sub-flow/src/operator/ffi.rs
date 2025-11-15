//! FFI operator implementation that bridges FFI operators with ReifyDB

use std::{
	cell::RefCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	slice::from_raw_parts,
};

use reifydb_core::{Row, interface::FlowNodeId};
use reifydb_engine::StandardRowEvaluator;
use reifydb_flow_operator_abi::{FFIOperatorDescriptor, FFIOperatorVTable, RowsFFI};
use reifydb_flow_operator_sdk::marshal::Marshaller;
use reifydb_type::RowNumber;

use crate::{
	Result,
	ffi::{
		callbacks::create_host_callbacks,
		conversion::{from_operator_sdk_change, to_operator_sdk_change},
		error::FFIError,
		transaction::TransactionHandle,
	},
	flow::{FlowChange, FlowChangeOrigin},
	operator::Operator,
	transaction::FlowTransaction,
};

/// FFI operator that wraps an external operator implementation
pub struct FFIOperator {
	/// Operator descriptor from the FFI library
	descriptor: FFIOperatorDescriptor,

	/// Virtual function table for calling FFI functions
	vtable: FFIOperatorVTable,

	/// Pointer to the FFI operator instance
	instance: *mut c_void,

	/// ID for this operator
	operator_id: FlowNodeId,

	/// Marshaller for type conversions
	marshaller: RefCell<Marshaller>,
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
			operator_id: node_id,
			marshaller: RefCell::new(Marshaller::new()),
		}
	}
}

impl Operator for FFIOperator {
	fn id(&self) -> FlowNodeId {
		self.operator_id
	}

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> Result<FlowChange> {
		// Lock the marshaller for this operation
		let mut marshaller = self.marshaller.borrow_mut();

		// Convert to operator-sdk FlowChange and marshal
		let operator_sdk_change = to_operator_sdk_change(&change);
		let ffi_input = marshaller.marshal_flow_change(&operator_sdk_change);

		// Create output holder
		let mut ffi_output = reifydb_flow_operator_abi::FlowChangeFFI::empty();

		// Create transaction handle
		let txn_handle = TransactionHandle::new(txn, self.operator_id, create_host_callbacks());
		let txn_handle_ptr = &txn_handle as *const _ as *mut reifydb_flow_operator_abi::TransactionHandle;

		let result = catch_unwind(AssertUnwindSafe(|| {
			(self.vtable.apply)(self.instance, txn_handle_ptr, &ffi_input, &mut ffi_output)
		}));

		// Handle panics from FFI code
		let result_code = match result {
			Ok(code) => code,
			Err(_) => {
				return Err(FFIError::Other("FFI operator panicked during apply".to_string()).into());
			}
		};

		// Check result code
		if result_code != 0 {
			return Err(
				FFIError::Other(format!("FFI operator apply failed with code: {}", result_code)).into()
			);
		}

		// Unmarshal the output and convert back to sub-flow FlowChange
		let operator_sdk_change =
			marshaller.unmarshal_flow_change(&ffi_output).map_err(|e| FFIError::Other(e))?;

		// Convert back with Internal origin since this came from an FFI operator
		Ok(from_operator_sdk_change(operator_sdk_change, FlowChangeOrigin::Internal(self.operator_id)))
	}

	fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Vec<Option<Row>>> {
		// Lock the marshaller for this operation
		let mut marshaller = self.marshaller.borrow_mut();

		// Convert row numbers to u64 array
		let row_numbers: Vec<u64> = rows.iter().map(|r| (*r).into()).collect();

		// Create output holder
		let mut ffi_output = RowsFFI {
			count: 0,
			rows: std::ptr::null_mut(),
		};

		// Create transaction handle
		let txn_handle = TransactionHandle::new(txn, self.operator_id, create_host_callbacks());
		let txn_handle_ptr = &txn_handle as *const _ as *mut reifydb_flow_operator_abi::TransactionHandle;

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
				return Err(FFIError::Other("FFI operator panicked during get_rows".to_string()).into());
			}
		};

		// Check result code
		if result_code != 0 {
			return Err(FFIError::Other(format!(
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
