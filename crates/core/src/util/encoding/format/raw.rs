// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::util::encoding::format::Formatter;

/// Formats raw byte slices without any decoding.
pub struct Raw;

impl Raw {
	/// Formats raw bytes as escaped ASCII strings.
	pub fn bytes(bytes: &[u8]) -> String {
		let escaped = bytes.iter().copied().flat_map(std::ascii::escape_default).collect::<Vec<_>>();
		format!("\"{}\"", String::from_utf8_lossy(&escaped))
	}
}

impl Formatter for Raw {
	fn key(key: &[u8]) -> String {
		Self::bytes(key)
	}

	fn value(_key: &[u8], value: &[u8]) -> String {
		Self::bytes(value)
	}
}
