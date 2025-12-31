// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Error types for streaming query execution.

use std::{fmt, sync::Arc};

use reifydb_type::diagnostic::Diagnostic;

/// Result type for stream items.
pub type StreamResult<T> = Result<T, StreamError>;

/// Errors that can occur during streaming query execution.
#[derive(Debug, Clone)]
pub enum StreamError {
	/// Query execution error from the engine.
	Query {
		/// The underlying diagnostic error.
		diagnostic: Arc<Diagnostic>,
		/// The statement that caused the error (if applicable).
		statement: Option<String>,
	},

	/// Stream was cancelled via cancellation token.
	Cancelled,

	/// Query execution exceeded timeout.
	Timeout,

	/// Internal channel error (producer dropped unexpectedly).
	Disconnected,
}

impl fmt::Display for StreamError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			StreamError::Query {
				diagnostic,
				..
			} => {
				// Use full diagnostic rendering for proper error display
				use reifydb_type::diagnostic::render::DefaultRenderer;
				let rendered = DefaultRenderer::render_string(diagnostic);
				write!(f, "{}", rendered)
			}
			StreamError::Cancelled => write!(f, "Query was cancelled"),
			StreamError::Timeout => write!(f, "Query execution timed out"),
			StreamError::Disconnected => write!(f, "Query stream disconnected unexpectedly"),
		}
	}
}

impl std::error::Error for StreamError {}

impl From<reifydb_type::Error> for StreamError {
	fn from(err: reifydb_type::Error) -> Self {
		StreamError::Query {
			diagnostic: Arc::new(err.diagnostic()),
			statement: None,
		}
	}
}

impl StreamError {
	/// Create a query error with a statement context.
	pub fn query_with_statement(err: reifydb_type::Error, statement: String) -> Self {
		let mut diagnostic = err.diagnostic();
		// Set the statement on the diagnostic so the renderer can display it
		diagnostic.with_statement(statement.clone());
		StreamError::Query {
			diagnostic: Arc::new(diagnostic),
			statement: Some(statement),
		}
	}
}
