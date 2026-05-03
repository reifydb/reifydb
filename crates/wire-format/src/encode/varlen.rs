// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{decimal::Decimal, frame::data::FrameColumnData, int::Int, r#type::Type, uint::Uint};

use super::EncodedColumn;
use crate::{
	encoding::{
		dict::{try_dict_encode_blob, try_dict_encode_bytes, try_dict_encode_utf8},
		rle::try_rle_encode_varlen,
	},
	format::Encoding,
};

pub(crate) fn try_dict_varlen(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Utf8(c) => {
			let dict = try_dict_encode_utf8(c, 0.5)?;
			Some(EncodedColumn {
				type_code: dict.type_code,
				encoding: Encoding::Dict,
				flags: dict.flags_bits,
				nones: vec![],
				data: dict.data,
				offsets: vec![],
				extra: dict.extra,
				row_count: 0,
			})
		}
		FrameColumnData::Blob(c) => {
			let dict = try_dict_encode_blob(c, 0.5)?;
			Some(EncodedColumn {
				type_code: dict.type_code,
				encoding: Encoding::Dict,
				flags: dict.flags_bits,
				nones: vec![],
				data: dict.data,
				offsets: vec![],
				extra: dict.extra,
				row_count: 0,
			})
		}
		FrameColumnData::Int(c) => {
			let slice: &[Int] = c;
			let serialized: Vec<Vec<u8>> = slice.iter().map(|v| v.0.to_signed_bytes_le()).collect();
			let dict = try_dict_encode_bytes(&serialized, Type::Int.to_u8(), 0.5)?;
			Some(EncodedColumn {
				type_code: dict.type_code,
				encoding: Encoding::Dict,
				flags: dict.flags_bits,
				nones: vec![],
				data: dict.data,
				offsets: vec![],
				extra: dict.extra,
				row_count: 0,
			})
		}
		FrameColumnData::Uint(c) => {
			let slice: &[Uint] = c;
			let serialized: Vec<Vec<u8>> = slice.iter().map(|v| v.0.to_signed_bytes_le()).collect();
			let dict = try_dict_encode_bytes(&serialized, Type::Uint.to_u8(), 0.5)?;
			Some(EncodedColumn {
				type_code: dict.type_code,
				encoding: Encoding::Dict,
				flags: dict.flags_bits,
				nones: vec![],
				data: dict.data,
				offsets: vec![],
				extra: dict.extra,
				row_count: 0,
			})
		}
		FrameColumnData::Decimal(c) => {
			let slice: &[Decimal] = c;
			let serialized: Vec<Vec<u8>> = slice.iter().map(|v| v.to_string().into_bytes()).collect();
			let dict = try_dict_encode_bytes(&serialized, Type::Decimal.to_u8(), 0.5)?;
			Some(EncodedColumn {
				type_code: dict.type_code,
				encoding: Encoding::Dict,
				flags: dict.flags_bits,
				nones: vec![],
				data: dict.data,
				offsets: vec![],
				extra: dict.extra,
				row_count: 0,
			})
		}
		_ => None,
	}
}

pub(crate) fn try_rle_varlen(inner: &FrameColumnData) -> Option<EncodedColumn> {
	match inner {
		FrameColumnData::Int(c) => {
			let slice: &[Int] = c;
			let serialized: Vec<Vec<u8>> = slice.iter().map(|v| v.0.to_signed_bytes_le()).collect();
			let encoded = try_rle_encode_varlen(&serialized)?;
			Some(EncodedColumn {
				type_code: Type::Int.to_u8(),
				encoding: Encoding::Rle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		FrameColumnData::Uint(c) => {
			let slice: &[Uint] = c;
			let serialized: Vec<Vec<u8>> = slice.iter().map(|v| v.0.to_signed_bytes_le()).collect();
			let encoded = try_rle_encode_varlen(&serialized)?;
			Some(EncodedColumn {
				type_code: Type::Uint.to_u8(),
				encoding: Encoding::Rle,
				flags: 0,
				nones: vec![],
				data: encoded,
				offsets: vec![],
				extra: vec![],
				row_count: 0,
			})
		}
		FrameColumnData::Decimal(c) => {
			let slice: &[Decimal] = c;
			let serialized: Vec<Vec<u8>> = slice.iter().map(|v| v.to_string().into_bytes()).collect();
			let encoded = try_rle_encode_varlen(&serialized)?;
			Some(EncodedColumn {
				type_code: Type::Decimal.to_u8(),
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
