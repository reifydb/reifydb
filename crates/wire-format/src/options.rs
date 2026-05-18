// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::format::Encoding;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionLevel {
	None,

	#[default]
	Fast,

	Max,
}

#[derive(Debug, Clone)]
pub struct EncodeOptions {
	pub compression: CompressionLevel,

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
