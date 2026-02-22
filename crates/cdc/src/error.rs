// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! CDC error types and diagnostics.

use std::fmt::Display;

use reifydb_core::common::CommitVersion;

/// Error type for CDC operations.
#[derive(Debug, Clone)]
pub enum CdcError {
	/// The operation failed due to an internal error.
	Internal(String),
	/// The CDC entry was not found.
	NotFound(CommitVersion),
	/// Encoding or decoding failed.
	Codec(String),
}

impl Display for CdcError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			CdcError::Internal(msg) => write!(f, "CDC storage internal error: {}", msg),
			CdcError::NotFound(version) => write!(f, "CDC entry not found: {:?}", version),
			CdcError::Codec(msg) => write!(f, "CDC codec error: {}", msg),
		}
	}
}

impl std::error::Error for CdcError {}

impl From<CdcError> for reifydb_type::error::Error {
	fn from(err: CdcError) -> Self {
		reifydb_type::error!(match err {
			CdcError::Internal(msg) => diagnostic::storage_error(msg),
			CdcError::NotFound(version) => diagnostic::not_found(version.0),
			CdcError::Codec(msg) => diagnostic::codec_error(msg),
		})
	}
}

/// Result type for CDC operations.
pub type CdcResult<T> = Result<T, CdcError>;

/// CDC-specific diagnostics.
pub mod diagnostic {
	use reifydb_type::{error::Diagnostic, fragment::Fragment};

	/// CDC storage operation failed
	pub fn storage_error(msg: impl Into<String>) -> Diagnostic {
		Diagnostic {
			code: "CDC_001".to_string(),
			statement: None,
			message: format!("CDC storage error: {}", msg.into()),
			column: None,
			fragment: Fragment::None,
			label: None,
			help: Some("Check CDC storage configuration and availability".to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		}
	}

	/// CDC entry not found at the specified version
	pub fn not_found(version: u64) -> Diagnostic {
		Diagnostic {
			code: "CDC_002".to_string(),
			statement: None,
			message: format!("CDC entry not found for version {}", version),
			column: None,
			fragment: Fragment::None,
			label: None,
			help: Some("The requested CDC version may have been garbage collected or never existed"
				.to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		}
	}

	/// CDC encoding/decoding failed
	pub fn codec_error(msg: impl Into<String>) -> Diagnostic {
		Diagnostic {
			code: "CDC_003".to_string(),
			statement: None,
			message: format!("CDC codec error: {}", msg.into()),
			column: None,
			fragment: Fragment::None,
			label: None,
			help: Some("This may indicate data corruption or version mismatch".to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		}
	}
}
