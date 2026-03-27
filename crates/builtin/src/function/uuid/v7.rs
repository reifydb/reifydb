// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::function::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{r#type::Type, uuid::Uuid7};
use uuid::{Builder, Uuid};

pub struct UuidV7;

impl UuidV7 {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for UuidV7 {
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
				let millis = ctx.runtime_context.clock.now_millis();
				let random_bytes = ctx.runtime_context.rng.bytes_10();
				let uuid = Uuid7::from(
					Builder::from_unix_timestamp_millis(millis, &random_bytes).into_uuid(),
				);
				data.push(uuid);
			}
			return Ok(ColumnData::uuid7(data));
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
					if parsed.get_version_num() != 7 {
						return Err(ScalarFunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!(
								"expected UUID v7, got v{}",
								parsed.get_version_num()
							),
						});
					}
					data.push(Uuid7::from(parsed));
				}
				Ok(ColumnData::uuid7(data))
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
		Type::Uuid7
	}
}
