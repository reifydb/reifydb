// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::container::{decimal_array::decimal_at, wide_int_array::wide_at};

pub(crate) const MIN_DIVISION_SCALE: u8 = 6;

pub(crate) fn numeric_to_f64(data: &ColumnBuffer, i: usize) -> Option<f64> {
	match data {
		ColumnBuffer::Int1(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Int2(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Int4(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Int8(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Int16(c) => wide_at::<i128>(c, i).map(|v| v as f64),
		ColumnBuffer::Uint1(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Uint2(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Uint4(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Uint8(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Uint16(c) => wide_at::<u128>(c, i).map(|v| v as f64),
		ColumnBuffer::Float4(c) => c.values().get(i).map(|&v| v as f64),
		ColumnBuffer::Float8(c) => c.values().get(i).copied(),
		ColumnBuffer::Decimal(c) => decimal_at(c, i).map(|v| v.to_f64()),
		_ => None,
	}
}
