// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct Defined;

impl Defined {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for Defined {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: columns.len(),
			});
		}

		let column = columns.get(0).unwrap();
		let data: Vec<bool> = (0..row_count).map(|i| column.data().is_defined(i)).collect();

		Ok(ColumnData::bool(data))
	}
}
