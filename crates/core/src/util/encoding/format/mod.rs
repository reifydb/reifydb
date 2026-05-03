// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

//! Pluggable formatters for human-readable key and value rendering.
//!
//! The `Formatter` trait takes a raw key byte slice and an optional value byte slice and returns a printable string;
//! the `raw` submodule provides the default hex rendering used by tools and tests. Implementors typically dispatch on
//! the leading `KeyKind` byte to produce a structured rendering of catalog keys.
pub mod raw;

pub trait Formatter {
	fn key(key: &[u8]) -> String;

	fn value(key: &[u8], value: &[u8]) -> String;

	fn key_value(key: &[u8], row: impl AsRef<[u8]>) -> String {
		Self::key_maybe_value(key, Some(row))
	}

	fn key_maybe_value(key: &[u8], value: Option<impl AsRef<[u8]>>) -> String {
		let fmtkey = Self::key(key);
		let fmtvalue = value.map_or("None".to_string(), |v| Self::value(key, v.as_ref()));
		format!("{fmtkey} => {fmtvalue}")
	}
}
