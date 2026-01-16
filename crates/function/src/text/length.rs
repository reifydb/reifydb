// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;

use crate::{ScalarFunction, ScalarFunctionContext};

pub struct TextLength;

impl TextLength {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for TextLength {
	fn scalar(&self, ctx: ScalarFunctionContext) -> reifydb_type::Result<ColumnData> {
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
