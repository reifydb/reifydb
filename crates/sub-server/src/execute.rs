// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Error types for query and command execution.
//!
//! The actual dispatch logic lives in [`crate::dispatch`]. This module
//! only defines the shared error types used by all transport handlers.

use std::{error, fmt, sync::Arc};

use reifydb_type::error::Diagnostic;

/// Error types for query/command execution.
#[derive(Debug)]
pub enum ExecuteError {
	/// Query exceeded the configured timeout.
	Timeout,
	/// Query was cancelled.
	Cancelled,
	/// Stream disconnected unexpectedly.
	Disconnected,
	/// Database engine returned an error with full diagnostic info.
	Engine {
		/// The full diagnostic with error code, source location, help text, etc.
		diagnostic: Arc<Diagnostic>,
		/// The statement that caused the error.
		statement: String,
	},
	/// Request was rejected by a request interceptor.
	Rejected {
		/// Error code for the rejection (e.g. "AUTH_REQUIRED", "INSUFFICIENT_CREDITS").
		code: String,
		/// Human-readable reason for rejection.
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

/// Result type for execute operations.
pub type ExecuteResult<T> = Result<T, ExecuteError>;
