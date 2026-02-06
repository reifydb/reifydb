// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use super::format_bytes::{format_bytes_internal, process_decimal_column, process_float_column, process_int_column};
use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

const SI_UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];

/// Formats bytes using SI/decimal units (1000-based: B, KB, MB, GB, TB, PB)
pub struct FormatBytesSi;

impl FormatBytesSi {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for FormatBytesSi {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
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

		match &column.data() {
			ColumnData::Int1(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Int2(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Int4(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Int8(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Uint1(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Uint2(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Uint4(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Uint8(container) => process_int_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Float4(container) => process_float_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Float8(container) => process_float_column!(container, row_count, 1000.0, &SI_UNITS),
			ColumnData::Decimal {
				container,
				..
			} => {
				process_decimal_column!(container, row_count, 1000.0, &SI_UNITS)
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
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
			}),
		}
	}
}
