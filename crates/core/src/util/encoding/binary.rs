// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_type::util::cowvec::CowVec;

pub fn decode_binary(s: &str) -> CowVec<u8> {
	let mut buf = [0; 4];
	let mut bytes = Vec::new();
	for c in s.chars() {
		match c as u32 {
			b @ 0x80..=0xff => bytes.push(b as u8),
			_ => bytes.extend(c.encode_utf8(&mut buf).as_bytes()),
		}
	}
	CowVec::new(bytes)
}
