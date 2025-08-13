// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	columnar::ColumnData,
	function::{ScalarFunction, ScalarFunctionContext},
};

pub struct Abs;

impl Abs {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Abs {
	fn scalar(
		&self,
		ctx: ScalarFunctionContext,
	) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Int1(container) => {
				let mut data =
					Vec::with_capacity(container.len());

				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						data.push(if *value < 0 {
							*value * -1
						} else {
							*value
						});
					}
				}

				Ok(ColumnData::int1_with_bitvec(
					data,
					container.bitvec(),
				))
			}
			ColumnData::Int2(container) => {
				let mut data =
					Vec::with_capacity(container.len());

				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						data.push(if *value < 0 {
							*value * -1
						} else {
							*value
						});
					}
				}

				Ok(ColumnData::int2_with_bitvec(
					data,
					container.bitvec(),
				))
			}
			_ => unimplemented!(),
		}
	}
}
