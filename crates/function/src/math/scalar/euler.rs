// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::f64::consts::E;

use reifydb_core::value::column::data::ColumnData;

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
};

pub struct Euler;

impl Euler {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for Euler {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if !ctx.columns.is_empty() {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		Ok(ColumnData::float8_with_bitvec(vec![E], vec![true]))
	}
}
