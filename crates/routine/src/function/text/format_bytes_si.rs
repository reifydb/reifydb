// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{
	function::text::format_bytes::{
		format_bytes_internal, process_decimal_column, process_float_column, process_int_column,
	},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
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

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::Int1(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnBuffer::Int2(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnBuffer::Int4(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnBuffer::Int8(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnBuffer::Uint1(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnBuffer::Uint2(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnBuffer::Uint4(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnBuffer::Uint8(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnBuffer::Float4(container) => {
				process_float_column!(container, row_count, 1000.0, &SI_UNITS)
			}
			ColumnBuffer::Float8(container) => {
				process_float_column!(container, row_count, 1000.0, &SI_UNITS)
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				process_decimal_column!(container, row_count, 1000.0, &SI_UNITS)
			}
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![
						Type::Int1,
						Type::Int2,
						Type::Int4,
						Type::Int8,
						Type::Uint1,
						Type::Uint2,
						Type::Uint4,
						Type::Uint8,
						Type::Float4,
						Type::Float8,
						Type::Decimal,
					],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match bitvec {
			Some(bv) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			None => result_data,
		};
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for FormatBytesSi {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
