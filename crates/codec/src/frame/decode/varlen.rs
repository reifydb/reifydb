// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{str, sync::Arc};

use arrow_array::{ArrayRef, LargeStringArray, builder::LargeBinaryBuilder};
use reifydb_value::value::{
	blob::Blob,
	container::{digest_array::push_digest, varlen_array::blob_array},
	digest::Digest,
	value_type::ValueType,
};

use super::column_type_from_code;
use crate::{error::DecodeError, reader::Reader, typeinfo::decode_digest_params};

pub(crate) fn decode_varlen_plain(
	type_code: u8,
	row_count: usize,
	data: &[u8],
	offsets: &[u8],
) -> Option<Result<ArrayRef, DecodeError>> {
	let ty = match column_type_from_code(type_code) {
		Ok(ty) => ty,
		Err(e) => return Some(Err(e)),
	};

	let result = match ty {
		ValueType::Utf8 => {
			let strings = decode_varlen_strings(data, offsets, row_count);
			match strings {
				Ok(s) => Ok(Arc::new(LargeStringArray::from(s)) as ArrayRef),
				Err(e) => Err(e),
			}
		}
		ValueType::Blob => {
			let blobs = decode_varlen_blobs(data, offsets, row_count);
			match blobs {
				Ok(b) => Ok(Arc::new(blob_array(&b)) as ArrayRef),
				Err(e) => Err(e),
			}
		}
		_ => return None,
	};

	Some(result)
}

pub(crate) fn decode_digest_plain(
	row_count: usize,
	data: &[u8],
	offsets: &[u8],
	extra: &[u8],
) -> Result<(ValueType, ArrayRef), DecodeError> {
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
	let offset_arr = decode_u32_offsets(offsets, row_count)?;
	let mut builder = LargeBinaryBuilder::with_capacity(row_count, data.len());
	for i in 0..row_count {
		let start = offset_arr[i];
		let end = offset_arr[i + 1];
		if start == end {
			builder.append_null();
			continue;
		}
		let digest = Digest::decode(checked_span(data, start, end)?)
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
	Ok((
		ValueType::Digest {
			inner: Box::new(inner),
			accuracy,
		},
		Arc::new(builder.finish()),
	))
}

fn decode_u32_offsets(offsets: &[u8], row_count: usize) -> Result<Vec<u32>, DecodeError> {
	let count = row_count + 1;
	let expected = count * 4;
	if offsets.len() < expected {
		return Err(DecodeError::UnexpectedEof {
			expected,
			available: offsets.len(),
		});
	}
	let mut result = Vec::with_capacity(count);
	for i in 0..count {
		result.push(u32::from_le_bytes([
			offsets[i * 4],
			offsets[i * 4 + 1],
			offsets[i * 4 + 2],
			offsets[i * 4 + 3],
		]));
	}
	Ok(result)
}

fn checked_span(data: &[u8], start: u32, end: u32) -> Result<&[u8], DecodeError> {
	data.get(start as usize..end as usize).ok_or(DecodeError::UnexpectedEof {
		expected: end as usize,
		available: data.len(),
	})
}

fn decode_varlen_strings(data: &[u8], offsets: &[u8], row_count: usize) -> Result<Vec<String>, DecodeError> {
	let offset_arr = decode_u32_offsets(offsets, row_count)?;
	let mut strings = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let bytes = checked_span(data, offset_arr[i], offset_arr[i + 1])?;
		let s = str::from_utf8(bytes).map_err(|e| DecodeError::InvalidData(format!("invalid UTF-8: {}", e)))?;
		strings.push(s.to_string());
	}
	Ok(strings)
}

fn decode_varlen_blobs(data: &[u8], offsets: &[u8], row_count: usize) -> Result<Vec<Blob>, DecodeError> {
	let offset_arr = decode_u32_offsets(offsets, row_count)?;
	let mut blobs = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let bytes = checked_span(data, offset_arr[i], offset_arr[i + 1])?;
		blobs.push(Blob::new(bytes.to_vec()));
	}
	Ok(blobs)
}
