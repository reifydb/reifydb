// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct Now;

impl Now {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Now {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let row_count = ctx.row_count;

		if ctx.columns.len() != 0 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		let millis = ctx.clock.now_millis() as i64;
		let data = vec![millis; row_count];
		let bitvec = vec![true; row_count];

		Ok(ColumnData::int8_with_bitvec(data, bitvec))
	}
}
