// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct IsAnonymous;

impl IsAnonymous {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for IsAnonymous {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if ctx.columns.len() != 0 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		let is_anonymous = ctx.identity.is_anonymous();
		let data: Vec<bool> = vec![is_anonymous; ctx.row_count];

		Ok(ColumnData::bool(data))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
	}
}
