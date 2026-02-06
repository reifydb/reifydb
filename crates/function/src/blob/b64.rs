// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	fragment::Fragment,
	value::{blob::Blob, r#type::Type},
};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct BlobB64;

impl BlobB64 {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for BlobB64 {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		// Validate exactly 1 argument
		if columns.len() != 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: columns.len(),
			});
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let b64_str = &container[i];
						let blob = Blob::from_b64(Fragment::internal(b64_str))?;
						result_data.push(blob);
						result_bitvec.push(true);
					} else {
						result_data.push(Blob::empty());
						result_bitvec.push(false);
					}
				}

				Ok(ColumnData::blob_with_bitvec(result_data, result_bitvec))
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
