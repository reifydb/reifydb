// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::{column::ColumnData, container::Utf8Container};

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct TextTrim;

impl TextTrim {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextTrim {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::utf8(Vec::<String>::new()));
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				max_bytes,
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let original_str = &container[i];
						let trimmed_str = original_str.trim();
						result_data.push(trimmed_str.to_string());
						result_bitvec.push(true);
					} else {
						result_data.push(String::new());
						result_bitvec.push(false);
					}
				}

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data, result_bitvec.into()),
					max_bytes: *max_bytes,
				})
			}
			_ => unimplemented!("text::trim only supports text input"),
		}
	}
}
