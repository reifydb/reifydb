// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub fn decode_binary(s: &str) -> Vec<u8> {
	let mut buf = [0; 4];
	let mut bytes = Vec::new();
	for c in s.chars() {
		match c as u32 {
			b @ 0x80..=0xff => bytes.push(b as u8),
			_ => bytes.extend(c.encode_utf8(&mut buf).as_bytes()),
		}
	}
	bytes
}
