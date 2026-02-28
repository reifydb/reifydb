// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, ScalarFunctionResult, error::ScalarFunctionError};

pub struct Id;

impl Id {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Id {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if ctx.columns.len() != 0 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		let identity = ctx.identity;
		if identity.is_anonymous() {
			return Ok(ColumnData::none_typed(Type::IdentityId, ctx.row_count));
		}

		Ok(ColumnData::identity_id(vec![identity; ctx.row_count]))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::IdentityId
	}
}
