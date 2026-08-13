// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
