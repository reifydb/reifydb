// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

/// Error returned when WAT text parsing fails.
#[derive(Debug)]
pub struct WatParseError(pub(crate) String);

impl core::fmt::Display for WatParseError {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		write!(f, "{}", self.0)
	}
}

/// The `WatParser` struct provides parsing of WebAssembly Text Format (WAT) into
/// binary WASM bytes.
///
/// Currently this is a stub implementation. When the `wat` crate dependency is available,
/// this will delegate to it for actual WAT-to-WASM conversion.
pub struct WatParser {}

impl WatParser {
	/// Parse a WAT string into WASM binary bytes.
	///
	/// # Errors
	///
	/// Returns a `WatParseError` because WAT parsing requires the `wat` feature
	/// which is not currently enabled.
	pub fn parse_str<T: AsRef<str>>(_wat: T) -> Result<Box<[u8]>> {
		Err(WatParseError("WAT text parsing requires the `wat` feature to be enabled".to_string()))
	}

	/// Parse a WAT file into WASM binary bytes.
	///
	/// # Errors
	///
	/// Returns a `WatParseError` because WAT parsing requires the `wat` feature
	/// which is not currently enabled.
	pub fn parse_file<T: AsRef<str>>(_path: T) -> Result<Box<[u8]>> {
		Err(WatParseError("WAT text parsing requires the `wat` feature to be enabled".to_string()))
	}
}

pub(crate) type Result<T, E = WatParseError> = core::result::Result<T, E>;
