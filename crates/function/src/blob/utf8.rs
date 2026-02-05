// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{fragment::Fragment, value::blob::Blob};

use crate::{ScalarFunction, ScalarFunctionContext};

pub struct BlobUtf8;

impl BlobUtf8 {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for BlobUtf8 {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::blob([]));
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());

				for i in 0..row_count {
					if container.is_defined(i) {
						let utf8_str = &container[i];
						let blob = Blob::from_utf8(Fragment::internal(utf8_str));
						result_data.push(blob);
					} else {
						result_data.push(Blob::empty())
					}
				}

				Ok(ColumnData::blob_with_bitvec(result_data, container.bitvec().clone()))
			}
			_ => unimplemented!("BlobUtf8 only supports text input"),
		}
	}
}
