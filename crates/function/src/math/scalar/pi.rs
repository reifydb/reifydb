// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct Pi;

impl Pi {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for Pi {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		if !ctx.columns.is_empty() {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		Ok(ColumnData::float8_with_bitvec(vec![std::f64::consts::PI], vec![true]))
	}
}
