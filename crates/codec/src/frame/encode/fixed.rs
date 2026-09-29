// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{
		decimal_array::DecimalView,
		temporal_array::{dates, datetimes, times},
		wide_int_array::wides,
	},
	value_type::ValueType,
};

use super::EncodedColumn;
use crate::{
	frame::{
		encoding::{
			delta::{
				try_delta_f32, try_delta_f64, try_delta_i8, try_delta_i16, try_delta_i32,
				try_delta_i64, try_delta_i128, try_delta_i256, try_delta_rle_f32, try_delta_rle_f64,
				try_delta_rle_i8, try_delta_rle_i16, try_delta_rle_i32, try_delta_rle_i64,
				try_delta_rle_i128, try_delta_rle_i256, try_delta_rle_u8, try_delta_rle_u16,
				try_delta_rle_u32, try_delta_rle_u64, try_delta_rle_u128, try_delta_u8, try_delta_u16,
				try_delta_u32, try_delta_u64, try_delta_u128,
			},
			rle::{try_rle_encode, try_rle_i32, try_rle_u64},
		},
		format::Encoding,
	},
	tag::ValueKind,
};

macro_rules! try_rle_fixed {
	($container:expr, $ty:expr, $elem_size:expr) => {
		try_rle_fixed!(slice: $container.values(), $ty, $elem_size)
	};
	(slice: $slice:expr, $ty:expr, $elem_size:expr) => {{
		let slice: &[_] = $slice;
		let encoded = try_rle_encode(slice, $elem_size, |v, buf| {
			buf.extend_from_slice(&v.to_le_bytes());
		})?;
		Some(EncodedColumn {
			type_code: ValueKind::of_type(&$ty).byte(),
			encoding: Encoding::Rle,
			flags: 0,
			nones: vec![],
			data: encoded,
			offsets: vec![],
			extra: vec![],
			row_count: 0,
		})
	}};
}

fn family<'a>(view: &ColumnView<'a>) -> Option<(ValueKind, DecimalView<'a>)> {
	match &view.data {
		ViewData::Decimal(c) => Some((ValueKind::Decimal, *c)),
		_ => None,
	}
}

fn family_column(kind: ValueKind, encoding: Encoding, data: Vec<u8>) -> EncodedColumn {
	EncodedColumn {
		type_code: kind.byte(),
		encoding,
		flags: 0,
		nones: vec![],
		data,
		offsets: vec![],
		extra: vec![],
		row_count: 0,
	}
}

pub(crate) fn try_rle_fixed(view: &ColumnView<'_>) -> Option<EncodedColumn> {
	if let Some((kind, array)) = family(view) {
		let data = match array {
			DecimalView::Decimal128(a) => {
				try_rle_encode(a.values(), 16, |v, buf| buf.extend_from_slice(&v.to_le_bytes()))?
			}
			DecimalView::Decimal256(a) => {
				try_rle_encode(a.values(), 32, |v, buf| buf.extend_from_slice(&v.to_le_bytes()))?
			}
		};
		return Some(family_column(kind, Encoding::Rle, data));
	}
	match &view.data {
		ViewData::Int1(c) => try_rle_fixed!(c, ValueType::Int1, 1),
		ViewData::Int2(c) => try_rle_fixed!(c, ValueType::Int2, 2),
		ViewData::Int4(c) => try_rle_fixed!(c, ValueType::Int4, 4),
		ViewData::Int8(c) => try_rle_fixed!(c, ValueType::Int8, 8),
		ViewData::Uint1(c) => try_rle_fixed!(c, ValueType::Uint1, 1),
		ViewData::Uint2(c) => try_rle_fixed!(c, ValueType::Uint2, 2),
		ViewData::Uint4(c) => try_rle_fixed!(c, ValueType::Uint4, 4),
		ViewData::Uint8(c) => try_rle_fixed!(c, ValueType::Uint8, 8),
		ViewData::Int16(c) => try_rle_fixed!(slice: &wides::<i128>(c), ValueType::Int16, 16),
		ViewData::Uint16(c) => try_rle_fixed!(slice: &wides::<u128>(c), ValueType::Uint16, 16),
		ViewData::Float4(c) => try_rle_fixed!(c, ValueType::Float4, 4),
		ViewData::Float8(c) => try_rle_fixed!(c, ValueType::Float8, 8),
		ViewData::Date(c) => {
			let raw: Vec<i32> = dates(c).iter().map(|d| d.to_days_since_epoch()).collect();
			let encoded = try_rle_i32(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::Date.byte(),
				encoding: Encoding::Rle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::DateTime(c) => {
			let raw: Vec<u64> = datetimes(c).iter().map(|d| d.to_nanos() as u64).collect();
			let encoded = try_rle_u64(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::DateTime.byte(),
				encoding: Encoding::Rle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Time(c) => {
			let raw: Vec<u64> = times(c).iter().map(|t| t.to_nanos_since_midnight()).collect();
			let encoded = try_rle_u64(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::Time.byte(),
				encoding: Encoding::Rle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		_ => None,
	}
}

pub(crate) fn try_delta_fixed(view: &ColumnView<'_>) -> Option<EncodedColumn> {
	if let Some((kind, array)) = family(view) {
		let data = match array {
			DecimalView::Decimal128(a) => try_delta_i128(a.values())?,
			DecimalView::Decimal256(a) => try_delta_i256(a.values())?,
		};
		return Some(family_column(kind, Encoding::Delta, data));
	}
	match &view.data {
		ViewData::Int1(c) => {
			let encoded = try_delta_i8(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Int1.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Int2(c) => {
			let encoded = try_delta_i16(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Int2.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Int4(c) => {
			let encoded = try_delta_i32(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Int4.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Int8(c) => {
			let encoded = try_delta_i64(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Int8.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint1(c) => {
			let encoded = try_delta_u8(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint1.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint2(c) => {
			let encoded = try_delta_u16(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint2.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint4(c) => {
			let encoded = try_delta_u32(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint4.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint8(c) => {
			let encoded = try_delta_u64(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint8.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Int16(c) => {
			let encoded = try_delta_i128(&wides::<i128>(c))?;
			Some(EncodedColumn {
				type_code: ValueKind::Int16.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint16(c) => {
			let encoded = try_delta_u128(&wides::<u128>(c))?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint16.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Float4(c) => {
			let encoded = try_delta_f32(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Float4.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Float8(c) => {
			let encoded = try_delta_f64(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Float8.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Date(c) => {
			let raw: Vec<i32> = dates(c).iter().map(|d| d.to_days_since_epoch()).collect();
			let encoded = try_delta_i32(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::Date.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::DateTime(c) => {
			let raw: Vec<i64> = datetimes(c).iter().map(|d| d.to_nanos()).collect();
			let encoded = try_delta_i64(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::DateTime.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Time(c) => {
			let raw: Vec<u64> = times(c).iter().map(|t| t.to_nanos_since_midnight()).collect();
			let encoded = try_delta_u64(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::Time.byte(),
				encoding: Encoding::Delta,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		_ => None,
	}
}

pub(crate) fn try_delta_rle_fixed(view: &ColumnView<'_>) -> Option<EncodedColumn> {
	if let Some((kind, array)) = family(view) {
		let data = match array {
			DecimalView::Decimal128(a) => try_delta_rle_i128(a.values())?,
			DecimalView::Decimal256(a) => try_delta_rle_i256(a.values())?,
		};
		return Some(family_column(kind, Encoding::DeltaRle, data));
	}
	match &view.data {
		ViewData::Int1(c) => {
			let encoded = try_delta_rle_i8(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Int1.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Int2(c) => {
			let encoded = try_delta_rle_i16(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Int2.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Int4(c) => {
			let encoded = try_delta_rle_i32(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Int4.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Int8(c) => {
			let encoded = try_delta_rle_i64(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Int8.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint1(c) => {
			let encoded = try_delta_rle_u8(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint1.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint2(c) => {
			let encoded = try_delta_rle_u16(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint2.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint4(c) => {
			let encoded = try_delta_rle_u32(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint4.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint8(c) => {
			let encoded = try_delta_rle_u64(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint8.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Int16(c) => {
			let encoded = try_delta_rle_i128(&wides::<i128>(c))?;
			Some(EncodedColumn {
				type_code: ValueKind::Int16.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Uint16(c) => {
			let encoded = try_delta_rle_u128(&wides::<u128>(c))?;
			Some(EncodedColumn {
				type_code: ValueKind::Uint16.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Float4(c) => {
			let encoded = try_delta_rle_f32(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Float4.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Float8(c) => {
			let encoded = try_delta_rle_f64(c.values())?;
			Some(EncodedColumn {
				type_code: ValueKind::Float8.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::DateTime(c) => {
			let raw: Vec<i64> = datetimes(c).iter().map(|d| d.to_nanos()).collect();
			let encoded = try_delta_rle_i64(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::DateTime.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Date(c) => {
			let raw: Vec<i32> = dates(c).iter().map(|d| d.to_days_since_epoch()).collect();
			let encoded = try_delta_rle_i32(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::Date.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		ViewData::Time(c) => {
			let raw: Vec<u64> = times(c).iter().map(|t| t.to_nanos_since_midnight()).collect();
			let encoded = try_delta_rle_u64(&raw)?;
			Some(EncodedColumn {
				type_code: ValueKind::Time.byte(),
				encoding: Encoding::DeltaRle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		_ => None,
	}
}
