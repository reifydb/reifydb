// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_runtime::clock::Clock;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, ScalarFunctionError};

pub struct Set;

impl Set {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Set {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: columns.len(),
			});
		}

		let column = columns.get(0).unwrap();

		let millis_u64: u64 = match column.data().get_as::<u64>(0) {
			Some(v) => v,
			None if !column.data().is_number() => {
				return Err(ScalarFunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
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
						Type::Int,
						Type::Uint,
					],
					actual: column.data().get_type(),
				});
			}
			None => {
				return Err(ScalarFunctionError::ExecutionFailed {
					function: ctx.fragment.clone(),
					reason: "clock::set requires a non-null argument".to_string(),
				});
			}
		};

		match ctx.clock {
			Clock::Mock(mock) => {
				mock.set_millis(millis_u64);
				let millis = mock.now_millis() as i64;
				let data = vec![millis; row_count];
				let bitvec = vec![true; row_count];
				Ok(ColumnData::int8_with_bitvec(data, bitvec))
			}
			Clock::Real => Err(ScalarFunctionError::ExecutionFailed {
				function: ctx.fragment.clone(),
				reason: "clock::set can only be used with a mock clock".to_string(),
			}),
		}
	}
}
