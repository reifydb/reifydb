// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0
pub mod raw;

/// Formats encoded keys and values.
pub trait Formatter {
	/// Formats a key.
	fn key(key: &[u8]) -> String;

	/// Formats a value. Also takes the key to determine the ty of value.
	fn value(key: &[u8], value: &[u8]) -> String;

	/// Formats a key/encoded pair.
	fn key_value(key: &[u8], row: impl AsRef<[u8]>) -> String {
		Self::key_maybe_value(key, Some(row))
	}

	/// Formats a key/encoded pair, where the value may not exist.
	fn key_maybe_value(key: &[u8], value: Option<impl AsRef<[u8]>>) -> String {
		let fmtkey = Self::key(key);
		let fmtvalue = value.map_or("None".to_string(), |v| Self::value(key, v.as_ref()));
		format!("{fmtkey} => {fmtvalue}")
	}
}
