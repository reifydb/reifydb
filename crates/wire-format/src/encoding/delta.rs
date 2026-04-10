// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Delta encoding for sorted/sequential integer and temporal columns.

use crate::error::DecodeError;

/// Try delta-encoding an i32 column. Returns None if not beneficial.
pub fn try_delta_i32(slice: &[i32]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i64> = slice.windows(2).map(|w| w[1] as i64 - w[0] as i64).collect();
	let width = delta_width(&deltas);

	// Delta size: 1 (width) + 4 (baseline) + (n-1) * width
	let delta_size = 1 + 4 + (slice.len() - 1) * width;
	let plain_size = slice.len() * 4;

	if delta_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(delta_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_deltas(&deltas, width, &mut buf);
	Some(buf)
}

/// Try delta-encoding an i64 column.
pub fn try_delta_i64(slice: &[i64]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i64> = slice.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();
	let width = delta_width(&deltas);

	let delta_size = 1 + 8 + (slice.len() - 1) * width;
	let plain_size = slice.len() * 8;

	if delta_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(delta_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_deltas(&deltas, width, &mut buf);
	Some(buf)
}

/// Try delta-encoding a u64 column (DateTime, Time).
pub fn try_delta_u64(slice: &[u64]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i64> = slice.windows(2).map(|w| w[1] as i64 - w[0] as i64).collect();
	let width = delta_width(&deltas);

	let delta_size = 1 + 8 + (slice.len() - 1) * width;
	let plain_size = slice.len() * 8;

	if delta_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(delta_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_deltas(&deltas, width, &mut buf);
	Some(buf)
}

/// Try DeltaRLE encoding on i32 values. Returns None if not beneficial.
pub fn try_delta_rle_i32(slice: &[i32]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i64> = slice.windows(2).map(|w| w[1] as i64 - w[0] as i64).collect();
	let width = delta_width(&deltas);
	let runs = rle_runs(&deltas);

	// DeltaRLE size: 1 (width) + 4 (baseline) + runs * (width + 4)
	let drle_size = 1 + 4 + runs.len() * (width + 4);
	let plain_size = slice.len() * 4;

	if drle_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(drle_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_delta_rle_runs(&runs, width, &mut buf);
	Some(buf)
}

/// Try DeltaRLE encoding on i64 values. Returns None if not beneficial.
pub fn try_delta_rle_i64(slice: &[i64]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i64> = slice.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();
	let width = delta_width(&deltas);
	let runs = rle_runs(&deltas);

	let drle_size = 1 + 8 + runs.len() * (width + 4);
	let plain_size = slice.len() * 8;

	if drle_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(drle_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_delta_rle_runs(&runs, width, &mut buf);
	Some(buf)
}

/// Try DeltaRLE encoding on u64 values (DateTime, Time).
pub fn try_delta_rle_u64(slice: &[u64]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i64> = slice.windows(2).map(|w| w[1] as i64 - w[0] as i64).collect();
	let width = delta_width(&deltas);
	let runs = rle_runs(&deltas);

	let drle_size = 1 + 8 + runs.len() * (width + 4);
	let plain_size = slice.len() * 8;

	if drle_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(drle_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_delta_rle_runs(&runs, width, &mut buf);
	Some(buf)
}

/// Decode delta-encoded i32 data.
pub fn decode_delta_i32(data: &[u8], row_count: usize) -> Result<Vec<i32>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 5 {
		return Err(DecodeError::InvalidData("delta i32 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 5;
	for _ in 1..row_count {
		let delta = read_signed_delta(&data[pos..], width);
		pos += width;
		let prev = *values.last().unwrap();
		values.push(prev.wrapping_add(delta as i32));
	}

	Ok(values)
}

/// Decode delta-encoded i64 data.
pub fn decode_delta_i64(data: &[u8], row_count: usize) -> Result<Vec<i64>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 9 {
		return Err(DecodeError::InvalidData("delta i64 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = i64::from_le_bytes([data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 9;
	for _ in 1..row_count {
		let delta = read_signed_delta(&data[pos..], width);
		pos += width;
		let prev = *values.last().unwrap();
		values.push(prev.wrapping_add(delta));
	}

	Ok(values)
}

/// Decode delta-encoded u64 data.
pub fn decode_delta_u64(data: &[u8], row_count: usize) -> Result<Vec<u64>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 9 {
		return Err(DecodeError::InvalidData("delta u64 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = u64::from_le_bytes([data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 9;
	for _ in 1..row_count {
		let delta = read_signed_delta(&data[pos..], width);
		pos += width;
		let prev = *values.last().unwrap();
		values.push((prev as i64).wrapping_add(delta) as u64);
	}

	Ok(values)
}

/// Decode DeltaRLE-encoded i32 data.
pub fn decode_delta_rle_i32(data: &[u8], row_count: usize) -> Result<Vec<i32>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 5 {
		return Err(DecodeError::InvalidData("delta_rle i32 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 5;
	while values.len() < row_count && pos + width + 4 <= data.len() {
		let delta = read_signed_delta(&data[pos..], width);
		pos += width;
		let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
		pos += 4;

		for _ in 0..count {
			if values.len() >= row_count {
				break;
			}
			let prev = *values.last().unwrap();
			values.push(prev.wrapping_add(delta as i32));
		}
	}

	if values.len() != row_count {
		return Err(DecodeError::InvalidData(format!(
			"delta_rle decoded {} values but expected {}",
			values.len(),
			row_count
		)));
	}

	Ok(values)
}

/// Decode DeltaRLE-encoded i64 data.
pub fn decode_delta_rle_i64(data: &[u8], row_count: usize) -> Result<Vec<i64>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 9 {
		return Err(DecodeError::InvalidData("delta_rle i64 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = i64::from_le_bytes([data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 9;
	while values.len() < row_count && pos + width + 4 <= data.len() {
		let delta = read_signed_delta(&data[pos..], width);
		pos += width;
		let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
		pos += 4;

		for _ in 0..count {
			if values.len() >= row_count {
				break;
			}
			let prev = *values.last().unwrap();
			values.push(prev.wrapping_add(delta));
		}
	}

	if values.len() != row_count {
		return Err(DecodeError::InvalidData(format!(
			"delta_rle decoded {} values but expected {}",
			values.len(),
			row_count
		)));
	}

	Ok(values)
}

/// Decode DeltaRLE-encoded u64 data.
pub fn decode_delta_rle_u64(data: &[u8], row_count: usize) -> Result<Vec<u64>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 9 {
		return Err(DecodeError::InvalidData("delta_rle u64 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = u64::from_le_bytes([data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 9;
	while values.len() < row_count && pos + width + 4 <= data.len() {
		let delta = read_signed_delta(&data[pos..], width);
		pos += width;
		let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
		pos += 4;

		for _ in 0..count {
			if values.len() >= row_count {
				break;
			}
			let prev = *values.last().unwrap();
			values.push((prev as i64).wrapping_add(delta) as u64);
		}
	}

	if values.len() != row_count {
		return Err(DecodeError::InvalidData(format!(
			"delta_rle decoded {} values but expected {}",
			values.len(),
			row_count
		)));
	}

	Ok(values)
}

/// Try delta-encoding an i128 column.
pub fn try_delta_i128(slice: &[i128]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i128> = slice.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();
	let width = delta_width_i128(&deltas);

	let delta_size = 1 + 16 + (slice.len() - 1) * width;
	let plain_size = slice.len() * 16;

	if delta_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(delta_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_deltas_i128(&deltas, width, &mut buf);
	Some(buf)
}

/// Try delta-encoding a u128 column.
pub fn try_delta_u128(slice: &[u128]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i128> = slice.windows(2).map(|w| w[1] as i128 - w[0] as i128).collect();
	let width = delta_width_i128(&deltas);

	let delta_size = 1 + 16 + (slice.len() - 1) * width;
	let plain_size = slice.len() * 16;

	if delta_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(delta_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_deltas_i128(&deltas, width, &mut buf);
	Some(buf)
}

/// Try DeltaRLE encoding on i128 values.
pub fn try_delta_rle_i128(slice: &[i128]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i128> = slice.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();
	let width = delta_width_i128(&deltas);
	let runs = rle_runs_i128(&deltas);

	let drle_size = 1 + 16 + runs.len() * (width + 4);
	let plain_size = slice.len() * 16;

	if drle_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(drle_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_delta_rle_runs_i128(&runs, width, &mut buf);
	Some(buf)
}

/// Try DeltaRLE encoding on u128 values.
pub fn try_delta_rle_u128(slice: &[u128]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i128> = slice.windows(2).map(|w| w[1] as i128 - w[0] as i128).collect();
	let width = delta_width_i128(&deltas);
	let runs = rle_runs_i128(&deltas);

	let drle_size = 1 + 16 + runs.len() * (width + 4);
	let plain_size = slice.len() * 16;

	if drle_size >= plain_size {
		return None;
	}

	let mut buf = Vec::with_capacity(drle_size);
	buf.push(width as u8);
	buf.extend_from_slice(&slice[0].to_le_bytes());
	encode_delta_rle_runs_i128(&runs, width, &mut buf);
	Some(buf)
}

/// Decode delta-encoded i128 data.
pub fn decode_delta_i128(data: &[u8], row_count: usize) -> Result<Vec<i128>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 17 {
		return Err(DecodeError::InvalidData("delta i128 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = i128::from_le_bytes([
		data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
		data[12], data[13], data[14], data[15], data[16],
	]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 17;
	for _ in 1..row_count {
		let delta = read_signed_delta_i128(&data[pos..], width);
		pos += width;
		let prev = *values.last().unwrap();
		values.push(prev.wrapping_add(delta));
	}

	Ok(values)
}

/// Decode delta-encoded u128 data.
pub fn decode_delta_u128(data: &[u8], row_count: usize) -> Result<Vec<u128>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 17 {
		return Err(DecodeError::InvalidData("delta u128 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = u128::from_le_bytes([
		data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
		data[12], data[13], data[14], data[15], data[16],
	]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 17;
	for _ in 1..row_count {
		let delta = read_signed_delta_i128(&data[pos..], width);
		pos += width;
		let prev = *values.last().unwrap();
		values.push((prev as i128).wrapping_add(delta) as u128);
	}

	Ok(values)
}

/// Decode DeltaRLE-encoded i128 data.
pub fn decode_delta_rle_i128(data: &[u8], row_count: usize) -> Result<Vec<i128>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 17 {
		return Err(DecodeError::InvalidData("delta_rle i128 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = i128::from_le_bytes([
		data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
		data[12], data[13], data[14], data[15], data[16],
	]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 17;
	while values.len() < row_count && pos + width + 4 <= data.len() {
		let delta = read_signed_delta_i128(&data[pos..], width);
		pos += width;
		let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
		pos += 4;

		for _ in 0..count {
			if values.len() >= row_count {
				break;
			}
			let prev = *values.last().unwrap();
			values.push(prev.wrapping_add(delta));
		}
	}

	if values.len() != row_count {
		return Err(DecodeError::InvalidData(format!(
			"delta_rle decoded {} values but expected {}",
			values.len(),
			row_count
		)));
	}

	Ok(values)
}

/// Decode DeltaRLE-encoded u128 data.
pub fn decode_delta_rle_u128(data: &[u8], row_count: usize) -> Result<Vec<u128>, DecodeError> {
	if row_count == 0 {
		return Ok(vec![]);
	}
	if data.len() < 17 {
		return Err(DecodeError::InvalidData("delta_rle u128 data too short".into()));
	}

	let width = data[0] as usize;
	let baseline = u128::from_le_bytes([
		data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
		data[12], data[13], data[14], data[15], data[16],
	]);

	let mut values = Vec::with_capacity(row_count);
	values.push(baseline);

	let mut pos = 17;
	while values.len() < row_count && pos + width + 4 <= data.len() {
		let delta = read_signed_delta_i128(&data[pos..], width);
		pos += width;
		let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
		pos += 4;

		for _ in 0..count {
			if values.len() >= row_count {
				break;
			}
			let prev = *values.last().unwrap();
			values.push((prev as i128).wrapping_add(delta) as u128);
		}
	}

	if values.len() != row_count {
		return Err(DecodeError::InvalidData(format!(
			"delta_rle decoded {} values but expected {}",
			values.len(),
			row_count
		)));
	}

	Ok(values)
}

fn delta_width(deltas: &[i64]) -> usize {
	let min = deltas.iter().copied().min().unwrap_or(0);
	let max = deltas.iter().copied().max().unwrap_or(0);

	if min >= i8::MIN as i64 && max <= i8::MAX as i64 {
		1
	} else if min >= i16::MIN as i64 && max <= i16::MAX as i64 {
		2
	} else if min >= i32::MIN as i64 && max <= i32::MAX as i64 {
		4
	} else {
		8
	}
}

fn encode_deltas(deltas: &[i64], width: usize, buf: &mut Vec<u8>) {
	for &d in deltas {
		write_signed_delta(d, width, buf);
	}
}

fn rle_runs(deltas: &[i64]) -> Vec<(i64, u32)> {
	if deltas.is_empty() {
		return vec![];
	}
	let mut runs = Vec::new();
	let mut current = deltas[0];
	let mut count: u32 = 1;

	for &d in &deltas[1..] {
		if d == current {
			count += 1;
		} else {
			runs.push((current, count));
			current = d;
			count = 1;
		}
	}
	runs.push((current, count));
	runs
}

fn encode_delta_rle_runs(runs: &[(i64, u32)], width: usize, buf: &mut Vec<u8>) {
	for &(delta, count) in runs {
		write_signed_delta(delta, width, buf);
		buf.extend_from_slice(&count.to_le_bytes());
	}
}

fn write_signed_delta(delta: i64, width: usize, buf: &mut Vec<u8>) {
	match width {
		1 => buf.push(delta as i8 as u8),
		2 => buf.extend_from_slice(&(delta as i16).to_le_bytes()),
		4 => buf.extend_from_slice(&(delta as i32).to_le_bytes()),
		8 => buf.extend_from_slice(&delta.to_le_bytes()),
		_ => unreachable!(),
	}
}

fn read_signed_delta(data: &[u8], width: usize) -> i64 {
	match width {
		1 => data[0] as i8 as i64,
		2 => i16::from_le_bytes([data[0], data[1]]) as i64,
		4 => i32::from_le_bytes([data[0], data[1], data[2], data[3]]) as i64,
		8 => i64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]),
		_ => unreachable!(),
	}
}

fn delta_width_i128(deltas: &[i128]) -> usize {
	let min = deltas.iter().copied().min().unwrap_or(0);
	let max = deltas.iter().copied().max().unwrap_or(0);

	if min >= i8::MIN as i128 && max <= i8::MAX as i128 {
		1
	} else if min >= i16::MIN as i128 && max <= i16::MAX as i128 {
		2
	} else if min >= i32::MIN as i128 && max <= i32::MAX as i128 {
		4
	} else if min >= i64::MIN as i128 && max <= i64::MAX as i128 {
		8
	} else {
		16
	}
}

fn encode_deltas_i128(deltas: &[i128], width: usize, buf: &mut Vec<u8>) {
	for &d in deltas {
		write_signed_delta_i128(d, width, buf);
	}
}

fn rle_runs_i128(deltas: &[i128]) -> Vec<(i128, u32)> {
	if deltas.is_empty() {
		return vec![];
	}
	let mut runs = Vec::new();
	let mut current = deltas[0];
	let mut count: u32 = 1;

	for &d in &deltas[1..] {
		if d == current {
			count += 1;
		} else {
			runs.push((current, count));
			current = d;
			count = 1;
		}
	}
	runs.push((current, count));
	runs
}

fn encode_delta_rle_runs_i128(runs: &[(i128, u32)], width: usize, buf: &mut Vec<u8>) {
	for &(delta, count) in runs {
		write_signed_delta_i128(delta, width, buf);
		buf.extend_from_slice(&count.to_le_bytes());
	}
}

fn write_signed_delta_i128(delta: i128, width: usize, buf: &mut Vec<u8>) {
	match width {
		1 => buf.push(delta as i8 as u8),
		2 => buf.extend_from_slice(&(delta as i16).to_le_bytes()),
		4 => buf.extend_from_slice(&(delta as i32).to_le_bytes()),
		8 => buf.extend_from_slice(&(delta as i64).to_le_bytes()),
		16 => buf.extend_from_slice(&delta.to_le_bytes()),
		_ => unreachable!(),
	}
}

fn read_signed_delta_i128(data: &[u8], width: usize) -> i128 {
	match width {
		1 => data[0] as i8 as i128,
		2 => i16::from_le_bytes([data[0], data[1]]) as i128,
		4 => i32::from_le_bytes([data[0], data[1], data[2], data[3]]) as i128,
		8 => i64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]])
			as i128,
		16 => i128::from_le_bytes([
			data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
			data[10], data[11], data[12], data[13], data[14], data[15],
		]),
		_ => unreachable!(),
	}
}
