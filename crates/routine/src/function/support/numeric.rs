// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{decimal_array::decimal_at, wide_int_array::wide_at},
};

pub(crate) const MIN_DIVISION_SCALE: u8 = 6;

pub(crate) fn numeric_to_f64(data: &ColumnView, i: usize) -> Option<f64> {
	match &data.data {
		ViewData::Int1(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Int2(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Int4(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Int8(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Int16(c) => wide_at::<i128>(c, i).map(|v| v as f64),
		ViewData::Uint1(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Uint2(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Uint4(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Uint8(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Uint16(c) => wide_at::<u128>(c, i).map(|v| v as f64),
		ViewData::Float4(c) => c.values().get(i).map(|&v| v as f64),
		ViewData::Float8(c) => c.values().get(i).copied(),
		ViewData::Decimal(c) => decimal_at(c, i).map(|v| v.to_f64()),
		_ => None,
	}
}
