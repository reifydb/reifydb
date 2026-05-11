// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub fn encode_u64_varint(v: u64, output: &mut Vec<u8>) {
	if v < (1 << 7) {
		output.push(v as u8);
	} else if v < (1 << 14) {
		output.push(0x80 | (v >> 8) as u8);
		output.push(v as u8);
	} else if v < (1 << 21) {
		output.push(0xc0 | (v >> 16) as u8);
		output.push((v >> 8) as u8);
		output.push(v as u8);
	} else if v < (1 << 28) {
		output.push(0xe0 | (v >> 24) as u8);
		output.push((v >> 16) as u8);
		output.push((v >> 8) as u8);
		output.push(v as u8);
	} else if v < (1 << 35) {
		output.push(0xf0 | (v >> 32) as u8);
		output.push((v >> 24) as u8);
		output.push((v >> 16) as u8);
		output.push((v >> 8) as u8);
		output.push(v as u8);
	} else if v < (1 << 42) {
		output.push(0xf8 | (v >> 40) as u8);
		output.push((v >> 32) as u8);
		output.push((v >> 24) as u8);
		output.push((v >> 16) as u8);
		output.push((v >> 8) as u8);
		output.push(v as u8);
	} else if v < (1 << 49) {
		output.push(0xfc | (v >> 48) as u8);
		output.push((v >> 40) as u8);
		output.push((v >> 32) as u8);
		output.push((v >> 24) as u8);
		output.push((v >> 16) as u8);
		output.push((v >> 8) as u8);
		output.push(v as u8);
	} else if v < (1 << 56) {
		output.push(0xfe | (v >> 56) as u8);
		output.push((v >> 48) as u8);
		output.push((v >> 40) as u8);
		output.push((v >> 32) as u8);
		output.push((v >> 24) as u8);
		output.push((v >> 16) as u8);
		output.push((v >> 8) as u8);
		output.push(v as u8);
	} else {
		output.push(0xff);
		output.extend_from_slice(&v.to_be_bytes());
	}
}

pub fn decode_u64_varint(input: &mut &[u8]) -> Option<u64> {
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

pub fn encode_i64_varint(v: i64, output: &mut Vec<u8>) {
	if v >= 0 {
		if v < 64 {
			output.push(0x80 | v as u8);
		} else if v < 8192 + 64 {
			let v = (v - 64) as u16;
			output.push(0xc0 | (v >> 8) as u8);
			output.push(v as u8);
		} else {
			output.push(0xfe);
			output.extend_from_slice(&v.to_be_bytes());
		}
	} else if v >= -64 {
		output.push(0x40 | (v + 64) as u8);
	} else if v >= -8192 - 64 {
		let v = (v + 64 + 8192) as u16;
		output.push(0x20 | (v >> 8) as u8);
		output.push(v as u8);
	} else {
		output.push(0x01);
		output.extend_from_slice(&v.to_be_bytes());
	}
}

pub fn decode_i64_varint(input: &mut &[u8]) -> Option<i64> {
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_u64_varint_ordering() {
		let values = vec![
			0,
			1,
			127,
			128,
			16383,
			16384,
			1000000,
			2000000,
			u32::MAX as u64,
			u32::MAX as u64 + 1,
			u64::MAX - 1,
			u64::MAX,
		];
		let mut encoded = Vec::new();
		for &v in &values {
			let mut buf = Vec::new();
			encode_u64_varint(v, &mut buf);
			encoded.push(buf);
		}

		for i in 0..values.len() - 1 {
			assert!(values[i] < values[i + 1]);
			assert!(encoded[i] < encoded[i + 1], "failed for {} vs {}", values[i], values[i + 1]);
		}
	}

	#[test]
	fn test_i64_varint_ordering() {
		let values = vec![
			i64::MIN,
			i64::MIN + 1,
			-1000000,
			-8257,
			-8256,
			-65,
			-64,
			-1,
			0,
			1,
			63,
			64,
			8255,
			8256,
			1000000,
			i64::MAX - 1,
			i64::MAX,
		];
		let mut encoded = Vec::new();
		for &v in &values {
			let mut buf = Vec::new();
			encode_i64_varint(v, &mut buf);
			encoded.push(buf);
		}

		for i in 0..values.len() - 1 {
			assert!(values[i] < values[i + 1], "values not sorted: {} vs {}", values[i], values[i + 1]);
			assert!(
				encoded[i] < encoded[i + 1],
				"encoded not sorted: {} ({:x?}) vs {} ({:x?})",
				values[i],
				encoded[i],
				values[i + 1],
				encoded[i + 1]
			);
		}
	}

	#[test]
	fn test_i64_varint_roundtrip() {
		let values = vec![i64::MIN, -1000000, -65, -64, -1, 0, 1, 63, 64, 1000000, i64::MAX];
		for v in values {
			let mut buf = Vec::new();
			encode_i64_varint(v, &mut buf);
			let mut slice = &buf[..];
			assert_eq!(decode_i64_varint(&mut slice), Some(v), "failed for {}", v);
			assert!(slice.is_empty());
		}
	}
}
