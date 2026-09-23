// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::error::DecodeError;

pub fn try_rle_encode<T: PartialEq + Copy>(
	slice: &[T],
	elem_size: usize,
	encode_value: fn(T, &mut Vec<u8>),
) -> Option<Vec<u8>> {
	if slice.is_empty() {
		return None;
	}

	let mut run_count = 1usize;
	let mut prev = slice[0];
	for &v in &slice[1..] {
		if v != prev {
			run_count += 1;
			prev = v;
		}
	}

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

	encode_value(current, &mut buf);
	buf.extend_from_slice(&count.to_le_bytes());

	Some(buf)
}

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

pub fn try_rle_i32(slice: &[i32]) -> Option<Vec<u8>> {
	try_rle_encode(slice, 4, |v, buf| buf.extend_from_slice(&v.to_le_bytes()))
}

pub fn try_rle_u64(slice: &[u64]) -> Option<Vec<u8>> {
	try_rle_encode(slice, 8, |v, buf| buf.extend_from_slice(&v.to_le_bytes()))
}

pub fn decode_rle_i32(data: &[u8], row_count: usize) -> Result<Vec<i32>, DecodeError> {
	decode_rle(data, row_count, 4, |b| i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

pub fn decode_rle_i64(data: &[u8], row_count: usize) -> Result<Vec<i64>, DecodeError> {
	decode_rle(data, row_count, 8, |b| i64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}

pub fn decode_rle_u64(data: &[u8], row_count: usize) -> Result<Vec<u64>, DecodeError> {
	decode_rle(data, row_count, 8, |b| u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}
