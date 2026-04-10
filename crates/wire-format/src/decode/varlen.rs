// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Decoding for variable-length column types: Utf8, Blob, Int, Uint, Decimal.

use std::str;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use reifydb_type::value::{
	blob::Blob,
	container::{blob::BlobContainer, number::NumberContainer, utf8::Utf8Container},
	decimal::Decimal,
	frame::data::FrameColumnData,
	int::Int,
	r#type::Type,
	uint::Uint,
};

use crate::{encoding::rle::decode_rle_varlen, error::DecodeError};

/// Decode a plain-encoded variable-length column.
/// Returns `None` if the type code is not a variable-length type.
pub(crate) fn decode_varlen_plain(
	type_code: u8,
	row_count: usize,
	data: &[u8],
	offsets: &[u8],
) -> Option<Result<FrameColumnData, DecodeError>> {
	let ty = Type::from_u8(type_code);

	let result = match ty {
		Type::Utf8 => {
			let strings = decode_varlen_strings(data, offsets, row_count);
			match strings {
				Ok(s) => Ok(FrameColumnData::Utf8(Utf8Container::new(s))),
				Err(e) => Err(e),
			}
		}
		Type::Blob => {
			let blobs = decode_varlen_blobs(data, offsets, row_count);
			match blobs {
				Ok(b) => Ok(FrameColumnData::Blob(BlobContainer::new(b))),
				Err(e) => Err(e),
			}
		}
		Type::Int => {
			let mut values = Vec::with_capacity(row_count);
			let offset_arr = decode_u32_offsets(offsets, row_count);
			for i in 0..row_count {
				let start = offset_arr[i] as usize;
				let end = offset_arr[i + 1] as usize;
				let big = BigInt::from_signed_bytes_le(&data[start..end]);
				values.push(Int(big));
			}
			Ok(FrameColumnData::Int(NumberContainer::new(values)))
		}
		Type::Uint => {
			let mut values = Vec::with_capacity(row_count);
			let offset_arr = decode_u32_offsets(offsets, row_count);
			for i in 0..row_count {
				let start = offset_arr[i] as usize;
				let end = offset_arr[i + 1] as usize;
				let big = BigInt::from_signed_bytes_le(&data[start..end]);
				values.push(Uint(big));
			}
			Ok(FrameColumnData::Uint(NumberContainer::new(values)))
		}
		Type::Decimal => decode_decimal(data, offsets, row_count),
		_ => return None,
	};

	Some(result)
}

/// Decode an RLE-encoded variable-length column.
pub(crate) fn decode_rle_varlen_column(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Result<FrameColumnData, DecodeError> {
	let ty = Type::from_u8(type_code);
	let entries = decode_rle_varlen(data, row_count)?;

	match ty {
		Type::Int => {
			let values: Vec<Int> =
				entries.into_iter().map(|bytes| Int(BigInt::from_signed_bytes_le(&bytes))).collect();
			Ok(FrameColumnData::Int(NumberContainer::new(values)))
		}
		Type::Uint => {
			let values: Vec<Uint> =
				entries.into_iter().map(|bytes| Uint(BigInt::from_signed_bytes_le(&bytes))).collect();
			Ok(FrameColumnData::Uint(NumberContainer::new(values)))
		}
		Type::Decimal => {
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
			Ok(FrameColumnData::Decimal(NumberContainer::new(values)))
		}
		_ => Err(DecodeError::InvalidData(format!("varlen RLE not supported for type {:?}", ty))),
	}
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
	Ok(FrameColumnData::Decimal(NumberContainer::new(values)))
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
