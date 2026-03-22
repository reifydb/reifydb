// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{r#type::Type, uuid::Uuid4};
use uuid::{Builder, Uuid};

use crate::{
	ScalarFunction, ScalarFunctionContext, ScalarFunctionResult, error::ScalarFunctionError, propagate_options,
};

pub struct UuidV4;

impl UuidV4 {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for UuidV4 {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let row_count = ctx.row_count;

		if ctx.columns.len() > 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: ctx.columns.len(),
			});
		}

		if ctx.columns.is_empty() {
			let mut data = Vec::with_capacity(row_count);
			for _ in 0..row_count {
				let bytes = ctx.runtime_context.rng.bytes_16();
				let uuid = Uuid4::from(Builder::from_random_bytes(bytes).into_uuid());
				data.push(uuid);
			}
			return Ok(ColumnData::uuid4(data));
		}

		let column = ctx.columns.get(0).unwrap();
		match &column.data() {
			ColumnData::Utf8 {
				container,
				..
			} => {
				let mut data = Vec::with_capacity(row_count);
				for i in 0..row_count {
					let s = &container[i];
					let parsed = Uuid::parse_str(s).map_err(|e| {
						ScalarFunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!("invalid UUID string '{}': {}", s, e),
						}
					})?;
					if parsed.get_version_num() != 4 {
						return Err(ScalarFunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!(
								"expected UUID v4, got v{}",
								parsed.get_version_num()
							),
						});
					}
					data.push(Uuid4::from(parsed));
				}
				Ok(ColumnData::uuid4(data))
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Uuid4
	}
}
