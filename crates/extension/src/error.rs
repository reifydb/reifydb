// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{error::Error, io};

use reifydb_sdk::error::SdkError;
use reifydb_type::error::Error as TypeError;

#[derive(Debug, thiserror::Error)]
pub enum ExtensionError {
	#[error("FFI load error: {0}")]
	FFILoad(String),

	#[error("WASM load error: {0}")]
	WasmLoad(String),

	#[error("API version mismatch: expected {expected}, got {actual}")]
	ApiVersionMismatch {
		expected: u32,
		actual: u32,
	},

	#[error("magic number mismatch for {kind}: expected {expected}, got {actual}")]
	MagicMismatch {
		kind: String,
		expected: u32,
		actual: u32,
	},

	#[error("extension '{name}' not found")]
	NotFound {
		name: String,
	},

	#[error("extension invocation failed: {0}")]
	Invocation(String),

	#[error("IO error: {0}")]
	Io(#[from] io::Error),

	#[error(transparent)]
	FFI(#[from] SdkError),

	#[error(transparent)]
	Other(Box<dyn Error + Send + Sync>),
}

impl From<ExtensionError> for TypeError {
	fn from(err: ExtensionError) -> Self {
		SdkError::Other(err.to_string()).into()
	}
}
