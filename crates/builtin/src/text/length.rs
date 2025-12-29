// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{ScalarFunction, ScalarFunctionContext},
	value::column::ColumnData,
};

pub struct TextLength;

impl TextLength {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextLength {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::int4(Vec::<i32>::new()));
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				..
			} => {
				let mut result = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let text = &container[i];
						// Return byte length, not character count
						result.push(text.len() as i32);
					} else {
						result.push(0);
					}
				}

				Ok(ColumnData::int4(result))
			}
			_ => unimplemented!("TextLength only supports text input"),
		}
	}
}
