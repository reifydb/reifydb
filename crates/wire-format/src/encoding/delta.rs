// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::error::DecodeError;

pub fn try_delta_i32(slice: &[i32]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i64> = slice.windows(2).map(|w| w[1] as i64 - w[0] as i64).collect();
	let width = delta_width(&deltas);

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

pub fn try_delta_rle_i32(slice: &[i32]) -> Option<Vec<u8>> {
	if slice.len() < 2 {
		return None;
	}

	let deltas: Vec<i64> = slice.windows(2).map(|w| w[1] as i64 - w[0] as i64).collect();
	let width = delta_width(&deltas);
	let runs = rle_runs(&deltas);

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

macro_rules! impl_delta_int128 {
	($ty:ident, $try_fn:ident, $try_rle_fn:ident, $dec_fn:ident, $dec_rle_fn:ident, $width:expr) => {
		pub fn $try_fn(slice: &[$ty]) -> Option<Vec<u8>> {
			if slice.len() < 2 {
				return None;
			}
			let deltas: Vec<i128> =
				slice.windows(2).map(|w| (w[1] as i128).wrapping_sub(w[0] as i128)).collect();
			let width = delta_width_i128(&deltas);
			let delta_size = 1 + $width + (slice.len() - 1) * width;
			if delta_size >= slice.len() * $width {
				return None;
			}
			let mut buf = Vec::with_capacity(delta_size);
			buf.push(width as u8);
			buf.extend_from_slice(&slice[0].to_le_bytes());
			encode_deltas_i128(&deltas, width, &mut buf);
			Some(buf)
		}
		pub fn $try_rle_fn(slice: &[$ty]) -> Option<Vec<u8>> {
			if slice.len() < 2 {
				return None;
			}
			let deltas: Vec<i128> =
				slice.windows(2).map(|w| (w[1] as i128).wrapping_sub(w[0] as i128)).collect();
			let width = delta_width_i128(&deltas);
			let runs = rle_runs_i128(&deltas);
			let drle_size = 1 + $width + runs.len() * (width + 4);
			if drle_size >= slice.len() * $width {
				return None;
			}
			let mut buf = Vec::with_capacity(drle_size);
			buf.push(width as u8);
			buf.extend_from_slice(&slice[0].to_le_bytes());
			encode_delta_rle_runs_i128(&runs, width, &mut buf);
			Some(buf)
		}
		pub fn $dec_fn(data: &[u8], row_count: usize) -> Result<Vec<$ty>, DecodeError> {
			if row_count == 0 {
				return Ok(vec![]);
			}
			if data.len() < 1 + $width {
				return Err(DecodeError::InvalidData("delta data too short".into()));
			}
			let width = data[0] as usize;
			let mut array = [0u8; $width];
			array.copy_from_slice(&data[1..1 + $width]);
			let baseline = $ty::from_le_bytes(array);
			let mut values = Vec::with_capacity(row_count);
			values.push(baseline);
			let mut pos = 1 + $width;
			for _ in 1..row_count {
				let delta = read_signed_delta_i128(&data[pos..], width);
				pos += width;
				let prev = *values.last().unwrap();
				values.push((prev as i128).wrapping_add(delta) as $ty);
			}
			Ok(values)
		}
		pub fn $dec_rle_fn(data: &[u8], row_count: usize) -> Result<Vec<$ty>, DecodeError> {
			if row_count == 0 {
				return Ok(vec![]);
			}
			if data.len() < 1 + $width {
				return Err(DecodeError::InvalidData("delta rle data too short".into()));
			}
			let width = data[0] as usize;
			let mut array = [0u8; $width];
			array.copy_from_slice(&data[1..1 + $width]);
			let baseline = $ty::from_le_bytes(array);
			let mut values = Vec::with_capacity(row_count);
			values.push(baseline);
			let mut pos = 1 + $width;
			while values.len() < row_count && pos + width + 4 <= data.len() {
				let delta = read_signed_delta_i128(&data[pos..], width);
				pos += width;
				let mut count_arr = [0u8; 4];
				count_arr.copy_from_slice(&data[pos..pos + 4]);
				let count = u32::from_le_bytes(count_arr) as usize;
				pos += 4;
				for _ in 0..count {
					if values.len() >= row_count {
						break;
					}
					let prev = *values.last().unwrap();
					values.push((prev as i128).wrapping_add(delta) as $ty);
				}
			}
			if values.len() != row_count {
				return Err(DecodeError::InvalidData("delta rle count mismatch".into()));
			}
			Ok(values)
		}
	};
}

impl_delta_int128!(i128, try_delta_i128, try_delta_rle_i128, decode_delta_i128, decode_delta_rle_i128, 16);
impl_delta_int128!(u128, try_delta_u128, try_delta_rle_u128, decode_delta_u128, decode_delta_rle_u128, 16);

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

macro_rules! impl_delta_int {
	($ty:ident, $try_fn:ident, $try_rle_fn:ident, $dec_fn:ident, $dec_rle_fn:ident, $width:expr) => {
		pub fn $try_fn(slice: &[$ty]) -> Option<Vec<u8>> {
			if slice.len() < 2 {
				return None;
			}
			let deltas: Vec<i64> =
				slice.windows(2).map(|w| (w[1] as i64).wrapping_sub(w[0] as i64)).collect();
			let width = delta_width(&deltas);
			let delta_size = 1 + $width + (slice.len() - 1) * width;
			if delta_size >= slice.len() * $width {
				return None;
			}
			let mut buf = Vec::with_capacity(delta_size);
			buf.push(width as u8);
			buf.extend_from_slice(&slice[0].to_le_bytes());
			encode_deltas(&deltas, width, &mut buf);
			Some(buf)
		}
		pub fn $try_rle_fn(slice: &[$ty]) -> Option<Vec<u8>> {
			if slice.len() < 2 {
				return None;
			}
			let deltas: Vec<i64> =
				slice.windows(2).map(|w| (w[1] as i64).wrapping_sub(w[0] as i64)).collect();
			let width = delta_width(&deltas);
			let runs = rle_runs(&deltas);
			let drle_size = 1 + $width + runs.len() * (width + 4);
			if drle_size >= slice.len() * $width {
				return None;
			}
			let mut buf = Vec::with_capacity(drle_size);
			buf.push(width as u8);
			buf.extend_from_slice(&slice[0].to_le_bytes());
			encode_delta_rle_runs(&runs, width, &mut buf);
			Some(buf)
		}
		pub fn $dec_fn(data: &[u8], row_count: usize) -> Result<Vec<$ty>, DecodeError> {
			if row_count == 0 {
				return Ok(vec![]);
			}
			if data.len() < 1 + $width {
				return Err(DecodeError::InvalidData("delta data too short".into()));
			}
			let width = data[0] as usize;
			let mut array = [0u8; $width];
			array.copy_from_slice(&data[1..1 + $width]);
			let baseline = $ty::from_le_bytes(array);
			let mut values = Vec::with_capacity(row_count);
			values.push(baseline);
			let mut pos = 1 + $width;
			for _ in 1..row_count {
				let delta = read_signed_delta(&data[pos..], width);
				pos += width;
				let prev = *values.last().unwrap();
				values.push((prev as i64).wrapping_add(delta) as $ty);
			}
			Ok(values)
		}
		pub fn $dec_rle_fn(data: &[u8], row_count: usize) -> Result<Vec<$ty>, DecodeError> {
			if row_count == 0 {
				return Ok(vec![]);
			}
			if data.len() < 1 + $width {
				return Err(DecodeError::InvalidData("delta rle data too short".into()));
			}
			let width = data[0] as usize;
			let mut array = [0u8; $width];
			array.copy_from_slice(&data[1..1 + $width]);
			let baseline = $ty::from_le_bytes(array);
			let mut values = Vec::with_capacity(row_count);
			values.push(baseline);
			let mut pos = 1 + $width;
			while values.len() < row_count && pos + width + 4 <= data.len() {
				let delta = read_signed_delta(&data[pos..], width);
				pos += width;
				let mut count_arr = [0u8; 4];
				count_arr.copy_from_slice(&data[pos..pos + 4]);
				let count = u32::from_le_bytes(count_arr) as usize;
				pos += 4;
				for _ in 0..count {
					if values.len() >= row_count {
						break;
					}
					let prev = *values.last().unwrap();
					values.push((prev as i64).wrapping_add(delta) as $ty);
				}
			}
			if values.len() != row_count {
				return Err(DecodeError::InvalidData("delta rle count mismatch".into()));
			}
			Ok(values)
		}
	};
}

macro_rules! impl_delta_float {
	($ty:ident, $uint_ty:ident, $try_fn:ident, $try_rle_fn:ident, $dec_fn:ident, $dec_rle_fn:ident, $width:expr) => {
		pub fn $try_fn(slice: &[$ty]) -> Option<Vec<u8>> {
			if slice.len() < 2 {
				return None;
			}
			let deltas: Vec<i64> = slice
				.windows(2)
				.map(|w| (w[1].to_bits() as i64).wrapping_sub(w[0].to_bits() as i64))
				.collect();
			let width = delta_width(&deltas);
			let delta_size = 1 + $width + (slice.len() - 1) * width;
			if delta_size >= slice.len() * $width {
				return None;
			}
			let mut buf = Vec::with_capacity(delta_size);
			buf.push(width as u8);
			buf.extend_from_slice(&slice[0].to_bits().to_le_bytes());
			encode_deltas(&deltas, width, &mut buf);
			Some(buf)
		}
		pub fn $try_rle_fn(slice: &[$ty]) -> Option<Vec<u8>> {
			if slice.len() < 2 {
				return None;
			}
			let deltas: Vec<i64> = slice
				.windows(2)
				.map(|w| (w[1].to_bits() as i64).wrapping_sub(w[0].to_bits() as i64))
				.collect();
			let width = delta_width(&deltas);
			let runs = rle_runs(&deltas);
			let drle_size = 1 + $width + runs.len() * (width + 4);
			if drle_size >= slice.len() * $width {
				return None;
			}
			let mut buf = Vec::with_capacity(drle_size);
			buf.push(width as u8);
			buf.extend_from_slice(&slice[0].to_bits().to_le_bytes());
			encode_delta_rle_runs(&runs, width, &mut buf);
			Some(buf)
		}
		pub fn $dec_fn(data: &[u8], row_count: usize) -> Result<Vec<$ty>, DecodeError> {
			if row_count == 0 {
				return Ok(vec![]);
			}
			if data.len() < 1 + $width {
				return Err(DecodeError::InvalidData("delta data too short".into()));
			}
			let width = data[0] as usize;
			let mut array = [0u8; $width];
			array.copy_from_slice(&data[1..1 + $width]);
			let baseline = $uint_ty::from_le_bytes(array);
			let mut values = Vec::with_capacity(row_count);
			values.push($ty::from_bits(baseline));
			let mut pos = 1 + $width;
			for _ in 1..row_count {
				let delta = read_signed_delta(&data[pos..], width);
				pos += width;
				let prev = values.last().unwrap().to_bits();
				values.push($ty::from_bits((prev as i64).wrapping_add(delta) as $uint_ty));
			}
			Ok(values)
		}
		pub fn $dec_rle_fn(data: &[u8], row_count: usize) -> Result<Vec<$ty>, DecodeError> {
			if row_count == 0 {
				return Ok(vec![]);
			}
			if data.len() < 1 + $width {
				return Err(DecodeError::InvalidData("delta rle data too short".into()));
			}
			let width = data[0] as usize;
			let mut array = [0u8; $width];
			array.copy_from_slice(&data[1..1 + $width]);
			let baseline = $uint_ty::from_le_bytes(array);
			let mut values = Vec::with_capacity(row_count);
			values.push($ty::from_bits(baseline));
			let mut pos = 1 + $width;
			while values.len() < row_count && pos + width + 4 <= data.len() {
				let delta = read_signed_delta(&data[pos..], width);
				pos += width;
				let mut count_arr = [0u8; 4];
				count_arr.copy_from_slice(&data[pos..pos + 4]);
				let count = u32::from_le_bytes(count_arr) as usize;
				pos += 4;
				for _ in 0..count {
					if values.len() >= row_count {
						break;
					}
					let prev = values.last().unwrap().to_bits();
					values.push($ty::from_bits((prev as i64).wrapping_add(delta) as $uint_ty));
				}
			}
			if values.len() != row_count {
				return Err(DecodeError::InvalidData("delta rle count mismatch".into()));
			}
			Ok(values)
		}
	};
}

impl_delta_int!(i8, try_delta_i8, try_delta_rle_i8, decode_delta_i8, decode_delta_rle_i8, 1);
impl_delta_int!(u8, try_delta_u8, try_delta_rle_u8, decode_delta_u8, decode_delta_rle_u8, 1);
impl_delta_int!(i16, try_delta_i16, try_delta_rle_i16, decode_delta_i16, decode_delta_rle_i16, 2);
impl_delta_int!(u16, try_delta_u16, try_delta_rle_u16, decode_delta_u16, decode_delta_rle_u16, 2);
impl_delta_int!(u32, try_delta_u32, try_delta_rle_u32, decode_delta_u32, decode_delta_rle_u32, 4);

impl_delta_float!(f32, u32, try_delta_f32, try_delta_rle_f32, decode_delta_f32, decode_delta_rle_f32, 4);
impl_delta_float!(f64, u64, try_delta_f64, try_delta_rle_f64, decode_delta_f64, decode_delta_rle_f64, 8);
