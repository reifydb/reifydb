// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::error::DecodeError;

/// Try to RLE-encode a slice of fixed-size values.
/// Returns None if RLE doesn't save space vs plain.
///
/// `encode_value` writes one value's LE bytes into buf.
pub fn try_rle_encode<T: PartialEq + Copy>(
	slice: &[T],
	elem_size: usize,
	encode_value: fn(T, &mut Vec<u8>),
) -> Option<Vec<u8>> {
	if slice.is_empty() {
		return None;
	}

	// Count runs
	let mut run_count = 1usize;
	let mut prev = slice[0];
	for &v in &slice[1..] {
		if v != prev {
			run_count += 1;
			prev = v;
		}
	}

	// RLE size = run_count * (elem_size + 4)
	// Plain size = slice.len() * elem_size
	let rle_size = run_count * (elem_size + 4);
	let plain_size = slice.len() * elem_size;

	if rle_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(rle_size);
	let mut current = slice[0];
	let mut count: u32 = 1;

	for &v in &slice[1..] {
		if v == current {
			count += 1;
		} else {
			encode_value(current, &mut buf);
			buf.extend_from_slice(&count.to_le_bytes());
			current = v;
			count = 1;
		}
	}
	// Write last run
	encode_value(current, &mut buf);
	buf.extend_from_slice(&count.to_le_bytes());

	Some(buf)
}

/// Decode RLE-encoded data back to a Vec of values.
///
/// `decode_value` reads one value from `data[pos..]` and returns `(value, bytes_consumed)`.
pub fn decode_rle<T: Copy>(
	data: &[u8],
	row_count: usize,
	elem_size: usize,
	decode_value: fn(&[u8]) -> T,
) -> Result<Vec<T>, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	let mut pos = 0;
	let run_size = elem_size + 4;

	while pos + run_size <= data.len() && values.len() < row_count {
		let value = decode_value(&data[pos..]);
		pos += elem_size;
		let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
		pos += 4;

		for _ in 0..count {
			if values.len() >= row_count {
				break;
			}
			values.push(value);
		}
	}

	if values.len() != row_count {
		return Err(DecodeError::InvalidData(format!(
			"RLE decoded {} values but expected {}",
			values.len(),
			row_count
		)));
	}

	Ok(values)
}

/// Try to RLE-encode a slice of variable-length byte vectors.
/// Returns None if RLE doesn't save space vs plain (with offsets).
///
/// Layout: repeated `(value_len: u32, value_bytes: [u8], run_count: u32)` pairs.
pub fn try_rle_encode_varlen(serialized: &[Vec<u8>]) -> Option<Vec<u8>> {
	if serialized.is_empty() {
		return None;
	}

	// Count runs and compute RLE size
	let mut rle_size = 4 + serialized[0].len() + 4; // first run
	let mut prev = &serialized[0];

	for v in &serialized[1..] {
		if v != prev {
			rle_size += 4 + v.len() + 4;
			prev = v;
		}
	}

	// Plain size: offsets (row_count + 1) * 4 + total data bytes
	let total_data: usize = serialized.iter().map(|v| v.len()).sum();
	let plain_size = (serialized.len() + 1) * 4 + total_data;

	if rle_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(rle_size);
	let mut current = &serialized[0];
	let mut count: u32 = 1;

	for v in &serialized[1..] {
		if v == current {
			count += 1;
		} else {
			buf.extend_from_slice(&(current.len() as u32).to_le_bytes());
			buf.extend_from_slice(current);
			buf.extend_from_slice(&count.to_le_bytes());
			current = v;
			count = 1;
		}
	}
	// Write last run
	buf.extend_from_slice(&(current.len() as u32).to_le_bytes());
	buf.extend_from_slice(current);
	buf.extend_from_slice(&count.to_le_bytes());

	Some(buf)
}

/// Decode variable-length RLE-encoded data back to byte vectors.
pub fn decode_rle_varlen(data: &[u8], row_count: usize) -> Result<Vec<Vec<u8>>, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	let mut pos = 0;

	while values.len() < row_count && pos + 4 <= data.len() {
		let value_len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
		pos += 4;

		if pos + value_len + 4 > data.len() {
			return Err(DecodeError::InvalidData("varlen RLE data truncated".to_string()));
		}

		let value = data[pos..pos + value_len].to_vec();
		pos += value_len;

		let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
		pos += 4;

		for _ in 0..count {
			if values.len() >= row_count {
				break;
			}
			values.push(value.clone());
		}
	}

	if values.len() != row_count {
		return Err(DecodeError::InvalidData(format!(
			"varlen RLE decoded {} values but expected {}",
			values.len(),
			row_count
		)));
	}

	Ok(values)
}

/// Convenience: try RLE for i32 slices (used by Date columns).
pub fn try_rle_i32(slice: &[i32]) -> Option<Vec<u8>> {
	try_rle_encode(slice, 4, |v, buf| buf.extend_from_slice(&v.to_le_bytes()))
}

/// Convenience: try RLE for i64 slices.
pub fn try_rle_i64(slice: &[i64]) -> Option<Vec<u8>> {
	try_rle_encode(slice, 8, |v, buf| buf.extend_from_slice(&v.to_le_bytes()))
}

/// Convenience: try RLE for u64 slices (used by DateTime, Time columns).
pub fn try_rle_u64(slice: &[u64]) -> Option<Vec<u8>> {
	try_rle_encode(slice, 8, |v, buf| buf.extend_from_slice(&v.to_le_bytes()))
}

/// Decode RLE for i32 values.
pub fn decode_rle_i32(data: &[u8], row_count: usize) -> Result<Vec<i32>, DecodeError> {
	decode_rle(data, row_count, 4, |b| i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

/// Decode RLE for i64 values.
pub fn decode_rle_i64(data: &[u8], row_count: usize) -> Result<Vec<i64>, DecodeError> {
	decode_rle(data, row_count, 8, |b| i64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}

/// Decode RLE for u64 values.
pub fn decode_rle_u64(data: &[u8], row_count: usize) -> Result<Vec<u64>, DecodeError> {
	decode_rle(data, row_count, 8, |b| u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}
