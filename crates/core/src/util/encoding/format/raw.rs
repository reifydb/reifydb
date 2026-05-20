// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::ascii;

use crate::util::encoding::format::Formatter;

pub struct Raw;

impl Raw {
	pub fn bytes(bytes: &[u8]) -> String {
		let escaped = bytes.iter().copied().flat_map(ascii::escape_default).collect::<Vec<_>>();
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
