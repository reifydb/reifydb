// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::function::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct Now;

impl Default for Now {
	fn default() -> Self {
		Self::new()
	}
}

impl Now {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Now {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let row_count = ctx.row_count;

		if !ctx.columns.is_empty() {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		let millis = ctx.runtime_context.clock.now_millis() as i64;
		let data = vec![millis; row_count];
		let bitvec = vec![true; row_count];

		Ok(ColumnData::int8_with_bitvec(data, bitvec))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int8
	}
}
