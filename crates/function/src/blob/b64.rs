// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{fragment::Fragment, value::blob::Blob};

use crate::{ScalarFunction, ScalarFunctionContext};

pub struct BlobB64;

impl BlobB64 {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for BlobB64 {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;
		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());

				for i in 0..row_count {
					if container.is_defined(i) {
						let b64_str = &container[i];
						let blob = Blob::from_b64(Fragment::internal(b64_str))?;
						result_data.push(blob);
					} else {
						result_data.push(Blob::empty())
					}
				}

				Ok(ColumnData::blob_with_bitvec(result_data, container.bitvec().clone()))
			}
			_ => unimplemented!("BlobB64 only supports text input"),
		}
	}
}
