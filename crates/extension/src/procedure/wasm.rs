// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use postcard::to_stdvec;
use reifydb_core::value::column::columns::Columns;
use reifydb_routine::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_sdk::{error::FFIError, marshal::wasm::unmarshal_columns_from_bytes};
use reifydb_type::{error::Error, value::r#type::Type};

use crate::{error::ExtensionError, loader::wasm::invoke_wasm_module};

fn ext_err(err: ExtensionError) -> RoutineError {
	RoutineError::Wrapped(Box::new(Error::from(FFIError::Other(err.to_string()))))
}

pub struct WasmProcedure {
	info: RoutineInfo,
	wasm_bytes: Vec<u8>,
}

impl WasmProcedure {
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
}

// SAFETY: WasmProcedure only holds inert data (name + bytes).

unsafe impl Send for WasmProcedure {}
unsafe impl Sync for WasmProcedure {}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for WasmProcedure {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let params_bytes = to_stdvec(ctx.params).map_err(|e| {
			ext_err(ExtensionError::Invocation(format!(
				"WASM procedure '{}' failed to serialize params: {}",
				self.info.name, e
			)))
		})?;

		let label = format!("WASM procedure '{}'", self.info.name);
		let output_bytes =
			invoke_wasm_module(&self.wasm_bytes, "procedure", &params_bytes, &label).map_err(ext_err)?;

		Ok(unmarshal_columns_from_bytes(&output_bytes))
	}
}
