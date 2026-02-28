// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
};

pub struct IsRoot;

impl IsRoot {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for IsRoot {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if ctx.columns.len() != 0 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		let is_root = ctx.identity.is_root();
		let data: Vec<bool> = vec![is_root; ctx.row_count];

		Ok(ColumnData::bool(data))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
	}
}
