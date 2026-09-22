// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str;

use arrow_array::{LargeStringArray, builder::LargeBinaryBuilder};
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use reifydb_value::value::{
	blob::Blob,
	container::{
		bignum_array::{decimal_array, int_array, uint_array},
		digest_array::{push_digest, push_none_slot},
		varlen_array::blob_array,
	},
	decimal::Decimal,
	digest::Digest,
	frame::data::FrameColumnData,
	int::Int,
	uint::Uint,
	value_type::ValueType,
};

use super::column_type_from_code;
use crate::{
	error::DecodeError, frame::encoding::rle::decode_rle_varlen, reader::Reader, typeinfo::decode_digest_params,
};

pub(crate) fn decode_varlen_plain(
	type_code: u8,
	row_count: usize,
	data: &[u8],
	offsets: &[u8],
) -> Option<Result<FrameColumnData, DecodeError>> {
	let ty = match column_type_from_code(type_code) {
		Ok(ty) => ty,
		Err(e) => return Some(Err(e)),
	};

	let result = match ty {
		ValueType::Utf8 => {
			let strings = decode_varlen_strings(data, offsets, row_count);
			match strings {
				Ok(s) => Ok(FrameColumnData::Utf8(LargeStringArray::from(s))),
				Err(e) => Err(e),
			}
		}
		ValueType::Blob => {
			let blobs = decode_varlen_blobs(data, offsets, row_count);
			match blobs {
				Ok(b) => Ok(FrameColumnData::Blob(blob_array(&b))),
				Err(e) => Err(e),
			}
		}
		ValueType::Int => {
			let mut values = Vec::with_capacity(row_count);
			let offset_arr = decode_u32_offsets(offsets, row_count);
			for i in 0..row_count {
				let start = offset_arr[i] as usize;
				let end = offset_arr[i + 1] as usize;
				let big = BigInt::from_signed_bytes_le(&data[start..end]);
				values.push(Int(big));
			}
			Ok(FrameColumnData::Int(int_array(values)))
		}
		ValueType::Uint => {
			let mut values = Vec::with_capacity(row_count);
			let offset_arr = decode_u32_offsets(offsets, row_count);
			for i in 0..row_count {
				let start = offset_arr[i] as usize;
				let end = offset_arr[i + 1] as usize;
				let big = BigInt::from_signed_bytes_le(&data[start..end]);
				values.push(Uint(big));
			}
			Ok(FrameColumnData::Uint(uint_array(values)))
		}
		ValueType::Decimal => decode_decimal(data, offsets, row_count),
		_ => return None,
	};

	Some(result)
}

pub(crate) fn decode_rle_varlen_column(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Result<FrameColumnData, DecodeError> {
	let ty = column_type_from_code(type_code)?;
	let entries = decode_rle_varlen(data, row_count)?;

	match ty {
		ValueType::Int => {
			let values: Vec<Int> =
				entries.into_iter().map(|bytes| Int(BigInt::from_signed_bytes_le(&bytes))).collect();
			Ok(FrameColumnData::Int(int_array(values)))
		}
		ValueType::Uint => {
			let values: Vec<Uint> =
				entries.into_iter().map(|bytes| Uint(BigInt::from_signed_bytes_le(&bytes))).collect();
			Ok(FrameColumnData::Uint(uint_array(values)))
		}
		ValueType::Decimal => {
			let mut values = Vec::with_capacity(row_count);
			for bytes in entries {
				let s = str::from_utf8(&bytes).map_err(|e| {
					DecodeError::InvalidData(format!("invalid decimal string: {}", e))
				})?;
				let dec: BigDecimal = s
					.parse()
					.map_err(|e| DecodeError::InvalidData(format!("invalid decimal: {}", e)))?;
				values.push(Decimal::new(dec));
			}
			Ok(FrameColumnData::Decimal(decimal_array(values)))
		}
		_ => Err(DecodeError::InvalidData(format!("varlen RLE not supported for type {:?}", ty))),
	}
}

pub(crate) fn decode_digest_plain(
	row_count: usize,
	data: &[u8],
	offsets: &[u8],
	extra: &[u8],
) -> Result<FrameColumnData, DecodeError> {
	let mut params = Reader::new(extra);
	let (inner, accuracy) = decode_digest_params(&mut params)?;
	if !params.is_empty() {
		return Err(DecodeError::InvalidData(format!(
			"digest column extra section has {} trailing bytes",
			params.remaining()
		)));
	}
	if offsets.len() != (row_count + 1) * 4 {
		return Err(DecodeError::InvalidData(format!(
			"digest column offsets length {} does not match row count {row_count}",
			offsets.len()
		)));
	}
	let offset_arr = decode_u32_offsets(offsets, row_count);
	let mut builder = LargeBinaryBuilder::with_capacity(row_count, data.len());
	for i in 0..row_count {
		let start = offset_arr[i] as usize;
		let end = offset_arr[i + 1] as usize;
		if start > end || end > data.len() {
			return Err(DecodeError::InvalidData(format!(
				"digest row {i} spans {start}..{end} outside {} data bytes",
				data.len()
			)));
		}
		if start == end {
			push_none_slot(&mut builder);
			continue;
		}
		let digest = Digest::decode(&data[start..end])
			.map_err(|error| DecodeError::InvalidData(format!("invalid digest in row {i}: {error}")))?;
		if *digest.inner() != inner || digest.accuracy() != accuracy {
			return Err(DecodeError::InvalidData(format!(
				"digest in row {i} is Digest({}, {}) but the column is Digest({inner}, {accuracy})",
				digest.inner(),
				digest.accuracy()
			)));
		}
		push_digest(&mut builder, &digest);
	}
	Ok(FrameColumnData::Digest {
		container: builder.finish(),
		inner,
		accuracy,
	})
}

fn decode_decimal(data: &[u8], offsets: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	let offset_arr = decode_u32_offsets(offsets, row_count);
	for i in 0..row_count {
		let start = offset_arr[i] as usize;
		let end = offset_arr[i + 1] as usize;
		let s = str::from_utf8(&data[start..end])
			.map_err(|e| DecodeError::InvalidData(format!("invalid decimal string: {}", e)))?;
		let dec: BigDecimal =
			s.parse().map_err(|e| DecodeError::InvalidData(format!("invalid decimal: {}", e)))?;
		values.push(Decimal::new(dec));
	}
	Ok(FrameColumnData::Decimal(decimal_array(values)))
}

fn decode_u32_offsets(offsets: &[u8], row_count: usize) -> Vec<u32> {
	let count = row_count + 1;
	let mut result = Vec::with_capacity(count);
	for i in 0..count {
		result.push(u32::from_le_bytes([
			offsets[i * 4],
			offsets[i * 4 + 1],
			offsets[i * 4 + 2],
			offsets[i * 4 + 3],
		]));
	}
	result
}

fn decode_varlen_strings(data: &[u8], offsets: &[u8], row_count: usize) -> Result<Vec<String>, DecodeError> {
	let offset_arr = decode_u32_offsets(offsets, row_count);
	let mut strings = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offset_arr[i] as usize;
		let end = offset_arr[i + 1] as usize;
		let s = str::from_utf8(&data[start..end])
			.map_err(|e| DecodeError::InvalidData(format!("invalid UTF-8: {}", e)))?;
		strings.push(s.to_string());
	}
	Ok(strings)
}

fn decode_varlen_blobs(data: &[u8], offsets: &[u8], row_count: usize) -> Result<Vec<Blob>, DecodeError> {
	let offset_arr = decode_u32_offsets(offsets, row_count);
	let mut blobs = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offset_arr[i] as usize;
		let end = offset_arr[i + 1] as usize;
		blobs.push(Blob::new(data[start..end].to_vec()));
	}
	Ok(blobs)
}
