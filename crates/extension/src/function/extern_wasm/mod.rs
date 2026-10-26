// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod loader;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, Schema};
use reifydb_core::value::{
	batch::take_rows,
	column::factory::{none, rename},
};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, broadcast::is_constant, context::FunctionContext,
	error::RoutineError,
};
use reifydb_sdk::common::extern_wasm::marshal::{marshal_columns_to_bytes, unmarshal_columns_from_bytes};
use reifydb_value::{
	fragment::Fragment,
	value::{system_columns::user_columns, value_type::ValueType},
};

use crate::loader::extern_wasm::invoke_extern_wasm_module;

pub struct ExternWasmScalarFunction {
	info: RoutineInfo,
	wasm_bytes: Vec<u8>,
}

impl ExternWasmScalarFunction {
	pub fn new(name: impl Into<String>, wasm_bytes: Vec<u8>) -> Self {
		let name = name.into();
		Self {
			info: RoutineInfo::new(&name),
			wasm_bytes,
		}
	}

	pub fn name(&self) -> &str {
		&self.info.name
	}

	fn err(&self, reason: impl Into<String>) -> RoutineError {
		RoutineError::FunctionExecutionFailed {
			function: Fragment::internal(&self.info.name),
			reason: reason.into(),
		}
	}

	fn broadcast(
		&self,
		args: &[(FieldRef, ArrayRef)],
		row_count: usize,
	) -> Result<Vec<(FieldRef, ArrayRef)>, RoutineError> {
		args.iter()
			.map(|(field, array)| {
				if !is_constant(array.len(), row_count) {
					return Ok((field.clone(), array.clone()));
				}
				let batch = RecordBatch::try_new(
					Arc::new(Schema::new(vec![field.clone()])),
					vec![array.clone()],
				)
				.map_err(|e| self.err(e.to_string()))?;
				Ok((field.clone(), take_rows(&batch, &vec![0; row_count])?.column(0).clone()))
			})
			.collect()
	}
}

// SAFETY: holds only a name and module bytes; each call instantiates the module fresh, sharing nothing.
unsafe impl Send for ExternWasmScalarFunction {}
unsafe impl Sync for ExternWasmScalarFunction {}

impl<'a> Routine<FunctionContext<'a>> for ExternWasmScalarFunction {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let args = self.broadcast(args, ctx.row_count)?;
		let input_bytes = marshal_columns_to_bytes(&args, ctx.row_count, &[])?;
		let label = format!("WASM scalar function '{}'", self.info.name);

		let output_bytes = invoke_extern_wasm_module(&self.wasm_bytes, "scalar", &input_bytes, &label)
			.map_err(|e| self.err(e.to_string()))?;

		let output_columns =
			unmarshal_columns_from_bytes(&output_bytes).map_err(|e| self.err(e.to_string()))?;

		match user_columns(&output_columns).next() {
			Some((field, array)) => Ok(rename((field.clone(), array.clone()), ctx.fragment.text())),
			None => Ok(none(ctx.fragment.text(), ctx.row_count)),
		}
	}
}

impl Function for ExternWasmScalarFunction {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Any
	}
}
