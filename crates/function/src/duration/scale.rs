// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DurationScale;

impl DurationScale {
	pub fn new() -> Self {
		Self
	}
}

fn extract_i64(data: &ColumnData, i: usize) -> Option<i64> {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int4(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int8(c) => c.get(i).copied(),
		ColumnData::Int16(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint16(c) => c.get(i).map(|&v| v as i64),
		_ => None,
	}
}

fn is_integer_type(data: &ColumnData) -> bool {
	matches!(
		data,
		ColumnData::Int1(_)
			| ColumnData::Int2(_) | ColumnData::Int4(_)
			| ColumnData::Int8(_) | ColumnData::Int16(_)
			| ColumnData::Uint1(_)
			| ColumnData::Uint2(_)
			| ColumnData::Uint4(_)
			| ColumnData::Uint8(_)
			| ColumnData::Uint16(_)
	)
}

impl ScalarFunction for DurationScale {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let dur_col = columns.get(0).unwrap();
		let scalar_col = columns.get(1).unwrap();

		match dur_col.data() {
			ColumnData::Duration(dur_container) => {
				if !is_integer_type(scalar_col.data()) {
					return Err(ScalarFunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: vec![
							Type::Int1,
							Type::Int2,
							Type::Int4,
							Type::Int8,
							Type::Int16,
							Type::Uint1,
							Type::Uint2,
							Type::Uint4,
							Type::Uint8,
							Type::Uint16,
						],
						actual: scalar_col.data().get_type(),
					});
				}

				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dur_container.get(i), extract_i64(scalar_col.data(), i)) {
						(Some(dur), Some(scalar)) => {
							container.push(*dur * scalar);
						}
						_ => container.push_default(),
					}
				}

				Ok(ColumnData::Duration(container))
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Duration
	}
}
