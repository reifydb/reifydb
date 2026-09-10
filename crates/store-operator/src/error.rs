// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error as StdError,
	fmt::{Display, Formatter, Result as FmtResult},
};

use reifydb_core::{error::diagnostic::internal::internal, interface::catalog::flow::FlowId};
use reifydb_value::error::Error;

pub type Result<T> = std::result::Result<T, OperatorError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorError {
	Backend {
		message: String,
	},
	CheckpointOutOfRange {
		flow: FlowId,
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
		}
	}
}

impl StdError for OperatorError {}

impl From<OperatorError> for Error {
	fn from(err: OperatorError) -> Self {
		Error(Box::new(internal(err.to_string())))
	}
}
