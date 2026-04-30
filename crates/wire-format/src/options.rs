// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::format::Encoding;

/// Controls the tradeoff between encode speed and compression ratio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionLevel {
	/// No compression: all columns use Plain encoding.
	None,
	/// Balanced: attempt dictionary, RLE, or delta if beneficial.
	#[default]
	Fast,
	/// Maximum compression: aggressively attempt all strategies.
	Max,
}

/// Options controlling how frames are encoded.
#[derive(Debug, Clone)]
pub struct EncodeOptions {
	/// Compression level for column encoding selection.
	pub compression: CompressionLevel,
	/// If set, forces all columns to use this encoding (if supported).
	pub force_encoding: Option<Encoding>,
}

impl Default for EncodeOptions {
	fn default() -> Self {
		EncodeOptions {
			compression: CompressionLevel::Fast,
			force_encoding: None,
		}
	}
}

impl EncodeOptions {
	pub fn none() -> Self {
		EncodeOptions {
			compression: CompressionLevel::None,
			force_encoding: None,
		}
	}

	pub fn fast() -> Self {
		EncodeOptions {
			compression: CompressionLevel::Fast,
			force_encoding: None,
		}
	}

	pub fn max() -> Self {
		EncodeOptions {
			compression: CompressionLevel::Max,
			force_encoding: None,
		}
	}

	pub fn forced(encoding: Encoding) -> Self {
		EncodeOptions {
			compression: CompressionLevel::Max,
			force_encoding: Some(encoding),
		}
	}
}
