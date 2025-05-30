// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::AsyncCowVec;

/// Decodes a raw byte vector from a Unicode string. Code points in the
/// range U+0080 to U+00FF are converted back to bytes 0x80 to 0xff.
/// This allows using e.g. \xff in the input string literal, and getting
/// back a 0xff byte in the byte vector. Otherwise, char(0xff) yields
/// the UTF-8 bytes 0xc3bf, which is the U+00FF code point as UTF-8.
/// These characters are effectively represented as ISO-8859-1 rather
/// than UTF-8, but it allows precise use of the entire u8 value range.
pub fn decode_binary(s: &str) -> AsyncCowVec<u8> {
    let mut buf = [0; 4];
    let mut bytes = Vec::new();
    for c in s.chars() {
        // u32 is the Unicode code point, not the UTF-8 encoding.
        match c as u32 {
            b @ 0x80..=0xff => bytes.push(b as u8),
            _ => bytes.extend(c.encode_utf8(&mut buf).as_bytes()),
        }
    }
    AsyncCowVec::new(bytes)
}
