// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type as ValueType};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
};

pub struct Type;

impl Type {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for Type {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
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
		let col_type = column.data().get_type();
		let type_name = col_type.to_string();

		let result_data: Vec<String> = vec![type_name; row_count];

		Ok(ColumnData::Utf8 {
			container: Utf8Container::new(result_data),
			max_bytes: MaxBytes::MAX,
		})
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}
}
