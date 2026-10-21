// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error as StdError,
	fmt::{Display, Formatter, Result as FmtResult},
};

use reifydb_core::{
	error::diagnostic::internal::internal,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator::state::KeyspaceId,
};
use reifydb_value::error::Error;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use rusqlite::Error as RusqliteError;

pub type Result<T> = std::result::Result<T, OperatorError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorError {
	Backend {
		message: String,
	},
	CheckpointOutOfRange {
		flow: FlowId,
	},
	Closed,
	SuffixWidth {
		operator: OperatorId,
		keyspace: KeyspaceId,
		expected: usize,
		found: usize,
	},
}

impl Display for OperatorError {
	fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
		match self {
			OperatorError::Backend {
				message,
			} => write!(f, "operator state backend failed: {message}"),
			OperatorError::CheckpointOutOfRange {
				flow,
			} => write!(f, "flow {} moved its checkpoint backwards", flow.0),
			OperatorError::Closed => write!(f, "operator state backend is closed"),
			OperatorError::SuffixWidth {
				operator,
				keyspace,
				expected,
				found,
			} => write!(
				f,
				"operator {} read keyspace {} with a {found} byte suffix, the keyspace is {expected} bytes",
				operator.0, keyspace.0
			),
		}
	}
}

impl StdError for OperatorError {}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl From<RusqliteError> for OperatorError {
	fn from(err: RusqliteError) -> Self {
		OperatorError::Backend {
			message: err.to_string(),
		}
	}
}

impl From<OperatorError> for Error {
	fn from(err: OperatorError) -> Self {
		Error(Box::new(internal(err.to_string())))
	}
}
