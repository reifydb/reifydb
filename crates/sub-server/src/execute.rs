// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error, fmt, sync::Arc};

use reifydb_type::error::Diagnostic;

#[derive(Debug)]
pub enum ExecuteError {
	Timeout,

	Cancelled,

	Disconnected,

	Engine {
		diagnostic: Arc<Diagnostic>,

		rql: String,
	},

	Rejected {
		code: String,

		message: String,
	},
}

impl fmt::Display for ExecuteError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ExecuteError::Timeout => write!(f, "Query execution timed out"),
			ExecuteError::Cancelled => write!(f, "Query was cancelled"),
			ExecuteError::Disconnected => write!(f, "Query stream disconnected unexpectedly"),
			ExecuteError::Engine {
				diagnostic,
				..
			} => write!(f, "Engine error: {}", diagnostic.message),
			ExecuteError::Rejected {
				code,
				message,
			} => write!(f, "Rejected [{}]: {}", code, message),
		}
	}
}

impl error::Error for ExecuteError {}

pub type ExecuteResult<T> = Result<T, ExecuteError>;
