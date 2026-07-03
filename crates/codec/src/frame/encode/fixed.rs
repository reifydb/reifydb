// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

use super::EncodedColumn;
use crate::{
	frame::{
		encoding::{
			delta::{
				try_delta_f32, try_delta_f64, try_delta_i8, try_delta_i16, try_delta_i32,
				try_delta_i64, try_delta_i128, try_delta_rle_f32, try_delta_rle_f64, try_delta_rle_i8,
				try_delta_rle_i16, try_delta_rle_i32, try_delta_rle_i64, try_delta_rle_i128,
				try_delta_rle_u8, try_delta_rle_u16, try_delta_rle_u32, try_delta_rle_u64,
				try_delta_rle_u128, try_delta_u8, try_delta_u16, try_delta_u32, try_delta_u64,
				try_delta_u128,
			},
			rle::{try_rle_encode, try_rle_i32, try_rle_u64},
		},
		format::Encoding,
	},
	tag::ValueKind,
};

macro_rules! try_rle_fixed {
	($container:expr, $ty:expr, $elem_size:expr) => {{
		let slice: &[_] = &**$container;
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

pub(crate) fn try_rle_fixed(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Int1(c) => try_rle_fixed!(c, ValueType::Int1, 1),
		FrameColumnData::Int2(c) => try_rle_fixed!(c, ValueType::Int2, 2),
		FrameColumnData::Int4(c) => try_rle_fixed!(c, ValueType::Int4, 4),
		FrameColumnData::Int8(c) => try_rle_fixed!(c, ValueType::Int8, 8),
		FrameColumnData::Uint1(c) => try_rle_fixed!(c, ValueType::Uint1, 1),
		FrameColumnData::Uint2(c) => try_rle_fixed!(c, ValueType::Uint2, 2),
		FrameColumnData::Uint4(c) => try_rle_fixed!(c, ValueType::Uint4, 4),
		FrameColumnData::Uint8(c) => try_rle_fixed!(c, ValueType::Uint8, 8),
		FrameColumnData::Int16(c) => try_rle_fixed!(c, ValueType::Int16, 16),
		FrameColumnData::Uint16(c) => try_rle_fixed!(c, ValueType::Uint16, 16),
		FrameColumnData::Float4(c) => try_rle_fixed!(c, ValueType::Float4, 4),
		FrameColumnData::Float8(c) => try_rle_fixed!(c, ValueType::Float8, 8),
		FrameColumnData::Date(c) => {
			let raw: Vec<i32> = (**c).iter().map(|d| d.to_days_since_epoch()).collect();
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
		FrameColumnData::DateTime(c) => {
			let raw: Vec<u64> = (**c).iter().map(|d| d.to_nanos()).collect();
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
		FrameColumnData::Time(c) => {
			let raw: Vec<u64> = (**c).iter().map(|t| t.to_nanos_since_midnight()).collect();
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

pub(crate) fn try_delta_fixed(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Int1(c) => {
			let encoded = try_delta_i8(c)?;
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
		FrameColumnData::Int2(c) => {
			let encoded = try_delta_i16(c)?;
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
		FrameColumnData::Int4(c) => {
			let encoded = try_delta_i32(c)?;
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
		FrameColumnData::Int8(c) => {
			let encoded = try_delta_i64(c)?;
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
		FrameColumnData::Uint1(c) => {
			let encoded = try_delta_u8(c)?;
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
		FrameColumnData::Uint2(c) => {
			let encoded = try_delta_u16(c)?;
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
		FrameColumnData::Uint4(c) => {
			let encoded = try_delta_u32(c)?;
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
		FrameColumnData::Uint8(c) => {
			let encoded = try_delta_u64(c)?;
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
		FrameColumnData::Int16(c) => {
			let encoded = try_delta_i128(c)?;
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
		FrameColumnData::Uint16(c) => {
			let encoded = try_delta_u128(c)?;
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
		FrameColumnData::Float4(c) => {
			let encoded = try_delta_f32(c)?;
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
		FrameColumnData::Float8(c) => {
			let encoded = try_delta_f64(c)?;
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
		FrameColumnData::Date(c) => {
			let raw: Vec<i32> = (**c).iter().map(|d| d.to_days_since_epoch()).collect();
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
		FrameColumnData::DateTime(c) => {
			let raw: Vec<u64> = (**c).iter().map(|d| d.to_nanos()).collect();
			let encoded = try_delta_u64(&raw)?;
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
		FrameColumnData::Time(c) => {
			let raw: Vec<u64> = (**c).iter().map(|t| t.to_nanos_since_midnight()).collect();
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

pub(crate) fn try_delta_rle_fixed(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Int1(c) => {
			let encoded = try_delta_rle_i8(c)?;
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
		FrameColumnData::Int2(c) => {
			let encoded = try_delta_rle_i16(c)?;
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
		FrameColumnData::Int4(c) => {
			let encoded = try_delta_rle_i32(c)?;
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
		FrameColumnData::Int8(c) => {
			let encoded = try_delta_rle_i64(c)?;
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
		FrameColumnData::Uint1(c) => {
			let encoded = try_delta_rle_u8(c)?;
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
		FrameColumnData::Uint2(c) => {
			let encoded = try_delta_rle_u16(c)?;
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
		FrameColumnData::Uint4(c) => {
			let encoded = try_delta_rle_u32(c)?;
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
		FrameColumnData::Uint8(c) => {
			let encoded = try_delta_rle_u64(c)?;
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
		FrameColumnData::Int16(c) => {
			let encoded = try_delta_rle_i128(c)?;
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
		FrameColumnData::Uint16(c) => {
			let encoded = try_delta_rle_u128(c)?;
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
		FrameColumnData::Float4(c) => {
			let encoded = try_delta_rle_f32(c)?;
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
		FrameColumnData::Float8(c) => {
			let encoded = try_delta_rle_f64(c)?;
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
		FrameColumnData::DateTime(c) => {
			let raw: Vec<u64> = (**c).iter().map(|d| d.to_nanos()).collect();
			let encoded = try_delta_rle_u64(&raw)?;
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
		FrameColumnData::Date(c) => {
			let raw: Vec<i32> = (**c).iter().map(|d| d.to_days_since_epoch()).collect();
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
		FrameColumnData::Time(c) => {
			let raw: Vec<u64> = (**c).iter().map(|t| t.to_nanos_since_midnight()).collect();
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
