// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	constraint::bytes::MaxBytes,
	container::decimal_array::decimals,
	value_type::ValueType,
};

use crate::function::{
	support::column::utf8_column,
	text::format_bytes::{format_bytes_internal, process_decimal_column, process_float_column, process_int_column},
};

const SI_UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];

pub struct FormatBytesSi {
	info: RoutineInfo,
}

impl Default for FormatBytesSi {
	fn default() -> Self {
		Self::new()
	}
}

impl FormatBytesSi {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::format_bytes_si"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for FormatBytesSi {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::Int1(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ViewData::Int2(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ViewData::Int4(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ViewData::Int8(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ViewData::Uint1(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ViewData::Uint2(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ViewData::Uint4(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ViewData::Uint8(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ViewData::Float4(container) => {
				process_float_column!(container, row_count, 1000.0, &SI_UNITS)
			}
			ViewData::Float8(container) => {
				process_float_column!(container, row_count, 1000.0, &SI_UNITS)
			}
			ViewData::Decimal(container) => {
				process_decimal_column!(ctx, container, row_count, 1000.0, &SI_UNITS)
			}
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![
						ValueType::Int1,
						ValueType::Int2,
						ValueType::Int4,
						ValueType::Int8,
						ValueType::Uint1,
						ValueType::Uint2,
						ValueType::Uint4,
						ValueType::Uint8,
						ValueType::Float4,
						ValueType::Float8,
						ValueType::DECIMAL,
					],
					actual: data.get_type(),
				});
			}
		};

		Ok(utf8_column(ctx.fragment.text(), MaxBytes::MAX, result_data))
	}
}

impl Function for FormatBytesSi {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
