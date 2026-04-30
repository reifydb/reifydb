// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{frame::data::FrameColumnData, r#type::Type};

use super::EncodedColumn;
use crate::{
	encoding::{
		delta::{
			try_delta_f32, try_delta_f64, try_delta_i8, try_delta_i16, try_delta_i32, try_delta_i64,
			try_delta_i128, try_delta_rle_f32, try_delta_rle_f64, try_delta_rle_i8, try_delta_rle_i16,
			try_delta_rle_i32, try_delta_rle_i64, try_delta_rle_i128, try_delta_rle_u8, try_delta_rle_u16,
			try_delta_rle_u32, try_delta_rle_u64, try_delta_rle_u128, try_delta_u8, try_delta_u16,
			try_delta_u32, try_delta_u64, try_delta_u128,
		},
		rle::{try_rle_encode, try_rle_i32, try_rle_u64},
	},
	format::Encoding,
};

macro_rules! try_rle_fixed {
	($container:expr, $ty:expr, $elem_size:expr) => {{
		let slice: &[_] = &**$container;
		let encoded = try_rle_encode(slice, $elem_size, |v, buf| {
			buf.extend_from_slice(&v.to_le_bytes());
		})?;
		Some(EncodedColumn {
			type_code: $ty.to_u8(),
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

/// Try RLE encoding for fixed-width types.
/// Returns None if the column type is not supported or RLE doesn't save space.
pub(crate) fn try_rle_fixed(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Int1(c) => try_rle_fixed!(c, Type::Int1, 1),
		FrameColumnData::Int2(c) => try_rle_fixed!(c, Type::Int2, 2),
		FrameColumnData::Int4(c) => try_rle_fixed!(c, Type::Int4, 4),
		FrameColumnData::Int8(c) => try_rle_fixed!(c, Type::Int8, 8),
		FrameColumnData::Uint1(c) => try_rle_fixed!(c, Type::Uint1, 1),
		FrameColumnData::Uint2(c) => try_rle_fixed!(c, Type::Uint2, 2),
		FrameColumnData::Uint4(c) => try_rle_fixed!(c, Type::Uint4, 4),
		FrameColumnData::Uint8(c) => try_rle_fixed!(c, Type::Uint8, 8),
		FrameColumnData::Int16(c) => try_rle_fixed!(c, Type::Int16, 16),
		FrameColumnData::Uint16(c) => try_rle_fixed!(c, Type::Uint16, 16),
		FrameColumnData::Float4(c) => try_rle_fixed!(c, Type::Float4, 4),
		FrameColumnData::Float8(c) => try_rle_fixed!(c, Type::Float8, 8),
		FrameColumnData::Date(c) => {
			let raw: Vec<i32> = (**c).iter().map(|d| d.to_days_since_epoch()).collect();
			let encoded = try_rle_i32(&raw)?;
			Some(EncodedColumn {
				type_code: Type::Date.to_u8(),
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
				type_code: Type::DateTime.to_u8(),
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
				type_code: Type::Time.to_u8(),
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

/// Try delta encoding for fixed-width types.
/// Returns None if the column type is not supported or delta doesn't save space.
pub(crate) fn try_delta_fixed(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Int1(c) => {
			let encoded = try_delta_i8(c)?;
			Some(EncodedColumn {
				type_code: Type::Int1.to_u8(),
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
				type_code: Type::Int2.to_u8(),
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
				type_code: Type::Int4.to_u8(),
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
				type_code: Type::Int8.to_u8(),
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
				type_code: Type::Uint1.to_u8(),
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
				type_code: Type::Uint2.to_u8(),
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
				type_code: Type::Uint4.to_u8(),
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
				type_code: Type::Uint8.to_u8(),
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
				type_code: Type::Int16.to_u8(),
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
				type_code: Type::Uint16.to_u8(),
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
				type_code: Type::Float4.to_u8(),
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
				type_code: Type::Float8.to_u8(),
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
				type_code: Type::Date.to_u8(),
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
				type_code: Type::DateTime.to_u8(),
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
				type_code: Type::Time.to_u8(),
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

/// Try delta-RLE encoding for fixed-width types.
/// Returns None if the column type is not supported or delta-RLE doesn't save space.
pub(crate) fn try_delta_rle_fixed(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Int1(c) => {
			let encoded = try_delta_rle_i8(c)?;
			Some(EncodedColumn {
				type_code: Type::Int1.to_u8(),
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
				type_code: Type::Int2.to_u8(),
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
				type_code: Type::Int4.to_u8(),
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
				type_code: Type::Int8.to_u8(),
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
				type_code: Type::Uint1.to_u8(),
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
				type_code: Type::Uint2.to_u8(),
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
				type_code: Type::Uint4.to_u8(),
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
				type_code: Type::Uint8.to_u8(),
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
				type_code: Type::Int16.to_u8(),
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
				type_code: Type::Uint16.to_u8(),
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
				type_code: Type::Float4.to_u8(),
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
				type_code: Type::Float8.to_u8(),
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
				type_code: Type::DateTime.to_u8(),
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
				type_code: Type::Date.to_u8(),
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
				type_code: Type::Time.to_u8(),
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
