// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI operator implementation that bridges FFI operators with ReifyDB

use std::{
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
};

use async_trait::async_trait;
use reifydb_abi::{ColumnsFFI, OperatorDescriptorFFI, OperatorVTableFFI};
use reifydb_core::{interface::FlowNodeId, value::column::Columns};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_sdk::{FFIError, FlowChange, marshal::Marshaller};
use reifydb_type::RowNumber;
use tokio::sync::RwLock;

use crate::{
	Result,
	ffi::{callbacks::create_host_callbacks, context::new_ffi_context},
	operator::{Operator, info::OperatorInfo},
	transaction::FlowTransaction,
};

/// FFI operator that wraps an external operator implementation
pub struct FFIOperator {
	/// Operator descriptor from the FFI library
	descriptor: OperatorDescriptorFFI,

	/// Virtual function table for calling FFI functions
	vtable: OperatorVTableFFI,

	/// Pointer to the FFI operator instance
	instance: *mut c_void,

	/// ID for this operator
	operator_id: FlowNodeId,

	/// Marshaller for type conversions
	marshaller: RwLock<Marshaller>,
}

// SAFETY: FFIOperator manages an FFI pointer but ensures proper synchronization
unsafe impl Send for FFIOperator {}
unsafe impl Sync for FFIOperator {}

impl FFIOperator {
	/// Create a new FFI operator
	pub fn new(descriptor: OperatorDescriptorFFI, instance: *mut c_void, operator_id: FlowNodeId) -> Self {
		let vtable = descriptor.vtable;

		Self {
			descriptor,
			vtable,
			instance,
			operator_id,
			marshaller: RwLock::new(Marshaller::new()),
		}
	}

	/// Get the operator descriptor
	pub(crate) fn descriptor(&self) -> &OperatorDescriptorFFI {
		&self.descriptor
	}
}

impl Drop for FFIOperator {
	fn drop(&mut self) {
		// Call the destroy function from the vtable to clean up the FFI operator instance
		if !self.instance.is_null() {
			(self.vtable.destroy)(self.instance);
		}
	}
}

impl OperatorInfo for FFIOperator {
	fn operator_name(&self) -> &'static str {
		// FFI operators have dynamic names, but OperatorInfo requires &'static str
		// We use a static placeholder; the actual name is in the descriptor
		"FFI"
	}

	fn operator_id(&self) -> FlowNodeId {
		self.operator_id
	}
}

#[async_trait]
impl Operator for FFIOperator {
	fn id(&self) -> FlowNodeId {
		self.operator_id
	}

	async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> Result<FlowChange> {
		// Lock the marshaller for this operation
		let mut marshaller = self.marshaller.write().await;

		// Marshal the flow change
		let ffi_input = marshaller.marshal_flow_change(&change);

		// Create output holder
		let mut ffi_output = reifydb_abi::FlowChangeFFI::empty();

		// Create FFI context
		let ffi_ctx = new_ffi_context(txn, self.operator_id, create_host_callbacks());
		let ffi_ctx_ptr = &ffi_ctx as *const _ as *mut reifydb_abi::ContextFFI;

		let result = catch_unwind(AssertUnwindSafe(|| {
			(self.vtable.apply)(self.instance, ffi_ctx_ptr, &ffi_input, &mut ffi_output)
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

		// Unmarshal the output
		let output_change = marshaller.unmarshal_flow_change(&ffi_output).map_err(|e| FFIError::Other(e))?;

		// Clear the marshaller's arena after operation
		marshaller.clear();

		Ok(output_change)
	}

	async fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		// Lock the marshaller for this operation
		let mut marshaller = self.marshaller.write().await;

		// Convert row numbers to u64 array
		let row_numbers: Vec<u64> = rows.iter().map(|r| (*r).into()).collect();

		// Create output holder
		let mut ffi_output = ColumnsFFI::empty();

		// Create FFI context
		let ffi_ctx = new_ffi_context(txn, self.operator_id, create_host_callbacks());
		let ffi_ctx_ptr = &ffi_ctx as *const _ as *mut reifydb_abi::ContextFFI;

		// Call FFI pull function
		let result = catch_unwind(AssertUnwindSafe(|| {
			(self.vtable.pull)(
				self.instance,
				ffi_ctx_ptr,
				row_numbers.as_ptr(),
				row_numbers.len(),
				&mut ffi_output,
			)
		}));

		// Handle panics from FFI code
		let result_code = match result {
			Ok(code) => code,
			Err(_) => {
				return Err(FFIError::Other("FFI operator panicked during pull".to_string()).into());
			}
		};

		// Check result code
		if result_code != 0 {
			return Err(
				FFIError::Other(format!("FFI operator pull failed with code: {}", result_code)).into()
			);
		}

		// Unmarshal the columns
		let columns = marshaller.unmarshal_columns(&ffi_output);

		// Clear the marshaller's arena after operation
		marshaller.clear();

		Ok(columns)
	}
}
