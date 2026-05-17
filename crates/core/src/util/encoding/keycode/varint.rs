// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(super) fn decode_u64_varint(input: &mut &[u8]) -> Option<u64> {
	if input.is_empty() {
		return None;
	}
	let first = input[0];
	let prefix = first.leading_ones() as usize;
	if prefix == 0 {
		*input = &input[1..];
		Some(first as u64)
	} else if prefix < 8 {
		if input.len() <= prefix {
			return None;
		}
		let mut v = if prefix == 7 {
			0
		} else {
			(first & (0xff >> (prefix + 1))) as u64
		};
		for i in 1..=prefix {
			v = (v << 8) | input[i] as u64;
		}
		*input = &input[prefix + 1..];
		Some(v)
	} else {
		if input.len() < 9 {
			return None;
		}
		let mut bytes = [0u8; 8];
		bytes.copy_from_slice(&input[1..9]);
		*input = &input[9..];
		Some(u64::from_be_bytes(bytes))
	}
}

pub(super) fn decode_i64_varint(input: &mut &[u8]) -> Option<i64> {
	if input.is_empty() {
		return None;
	}
	let first = input[0];
	if first >= 0x80 {
		if first < 0xc0 {
			*input = &input[1..];
			Some((first & 0x3f) as i64)
		} else if first < 0xfe {
			if input.len() < 2 {
				return None;
			}
			let v = ((first & 0x1f) as u16) << 8 | input[1] as u16;
			*input = &input[2..];
			Some(v as i64 + 64)
		} else {
			if input.len() < 9 {
				return None;
			}
			let mut bytes = [0u8; 8];
			bytes.copy_from_slice(&input[1..9]);
			*input = &input[9..];
			Some(i64::from_be_bytes(bytes))
		}
	} else if first >= 0x40 {
		*input = &input[1..];
		Some((first & 0x3f) as i64 - 64)
	} else if first >= 0x20 {
		if input.len() < 2 {
			return None;
		}
		let v = ((first & 0x1f) as u16) << 8 | input[1] as u16;
		*input = &input[2..];
		Some(v as i64 - 64 - 8192)
	} else {
		if input.len() < 9 {
			return None;
		}
		let mut bytes = [0u8; 8];
		bytes.copy_from_slice(&input[1..9]);
		*input = &input[9..];
		Some(i64::from_be_bytes(bytes))
	}
}
