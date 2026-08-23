// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error::Error as StdError, fmt, fmt::Display};

use reifydb_codec::error::{DecodeError, EncodeError};
use reifydb_core::common::CommitVersion;
use reifydb_value::{
	error,
	error::{Diagnostic, Error},
};

#[derive(Debug, Clone)]
pub enum CdcError {
	Internal(String),

	NotFound(CommitVersion),

	DuplicateVersion(CommitVersion),

	Codec(String),

	Storage(Box<Diagnostic>),
}

impl Display for CdcError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CdcError::Internal(msg) => write!(f, "CDC storage internal error: {}", msg),
			CdcError::NotFound(version) => write!(f, "CDC entry not found: {:?}", version),
			CdcError::DuplicateVersion(version) => {
				write!(f, "CDC version already written: {:?}", version)
			}
			CdcError::Codec(msg) => write!(f, "CDC codec error: {}", msg),
			CdcError::Storage(diagnostic) => write!(f, "CDC storage error: {}", diagnostic.message),
		}
	}
}

impl StdError for CdcError {}

impl From<EncodeError> for CdcError {
	fn from(err: EncodeError) -> Self {
		CdcError::Codec(err.to_string())
	}
}

impl From<DecodeError> for CdcError {
	fn from(err: DecodeError) -> Self {
		CdcError::Codec(err.to_string())
	}
}

impl From<Error> for CdcError {
	fn from(err: Error) -> Self {
		CdcError::Storage(err.0)
	}
}

impl From<CdcError> for Error {
	fn from(err: CdcError) -> Self {
		if let CdcError::Storage(diagnostic) = err {
			return Error(diagnostic);
		}
		error!(match err {
			CdcError::Internal(msg) => diagnostic::storage_error(msg),
			CdcError::NotFound(version) => diagnostic::not_found(version.0),
			CdcError::DuplicateVersion(version) => diagnostic::duplicate_version(version.0),
			CdcError::Codec(msg) => diagnostic::codec_error(msg),
			CdcError::Storage(_) => unreachable!(),
		})
	}
}

pub type CdcResult<T> = Result<T, CdcError>;

pub mod diagnostic {

	use reifydb_value::{error::Diagnostic, fragment::Fragment};

	pub fn storage_error(msg: impl Into<String>) -> Diagnostic {
		Diagnostic {
			code: "CDC_001".to_string(),
			rql: None,
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

	pub fn not_found(version: u64) -> Diagnostic {
		Diagnostic {
			code: "CDC_002".to_string(),
			rql: None,
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

	pub fn duplicate_version(version: u64) -> Diagnostic {
		Diagnostic {
			code: "CDC_005".to_string(),
			rql: None,
			message: format!("CDC version {} has already been written", version),
			column: None,
			fragment: Fragment::None,
			label: None,
			help: Some("The CDC log carries exactly one record per commit version and a sealed block \
				    is never rewritten"
				.to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		}
	}

	pub fn consumer_overtaken(consumer: impl Into<String>, cursor: u64, truncated_before: u64) -> Diagnostic {
		Diagnostic {
			code: "CDC_004".to_string(),
			rql: None,
			message: format!(
				"CDC consumer '{}' was overtaken by retention: its cursor {} is below the truncation \
				 floor {}, the changes in between are gone",
				consumer.into(),
				cursor,
				truncated_before
			),
			column: None,
			fragment: Fragment::None,
			label: None,
			help: Some("The consumer lagged past the CDC TTL window and must resync from a fresh \
				    snapshot instead of resuming its cursor"
				.to_string()),
			notes: vec![],
			cause: None,
			operator_chain: None,
		}
	}

	pub fn codec_error(msg: impl Into<String>) -> Diagnostic {
		Diagnostic {
			code: "CDC_003".to_string(),
			rql: None,
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
