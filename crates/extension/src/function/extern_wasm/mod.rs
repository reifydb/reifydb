// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod loader;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_sdk::marshal::extern_wasm::{marshal_columns_to_bytes, unmarshal_columns_from_bytes};
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

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

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let input_bytes = marshal_columns_to_bytes(args);
		let label = format!("WASM scalar function '{}'", self.info.name);

		let output_bytes = invoke_extern_wasm_module(&self.wasm_bytes, "scalar", &input_bytes, &label)
			.map_err(|e| self.err(e.to_string()))?;

		let output_columns = unmarshal_columns_from_bytes(&output_bytes);

		match output_columns.first() {
			Some(col) => {
				let data = col.data().clone();
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), data)]))
			}
			None => {
				let data = ColumnBuffer::none_typed(ValueType::Any, args.row_count());
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), data)]))
			}
		}
	}
}

impl Function for ExternWasmScalarFunction {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
