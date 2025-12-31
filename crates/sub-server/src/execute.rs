// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Query and command execution with async streaming.
//!
//! This module provides async wrappers around the database engine operations.
//! The engine internally uses spawn_blocking for sync execution, streaming
//! results back through async channels.

use std::{sync::Arc, time::Duration};

use futures_util::TryStreamExt;
use reifydb_core::{
	Frame,
	interface::{Engine, Identity},
	stream::StreamError,
};
use reifydb_engine::StandardEngine;
use reifydb_type::{Params, diagnostic::Diagnostic};

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
}

impl std::fmt::Display for ExecuteError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ExecuteError::Timeout => write!(f, "Query execution timed out"),
			ExecuteError::Cancelled => write!(f, "Query was cancelled"),
			ExecuteError::Disconnected => write!(f, "Query stream disconnected unexpectedly"),
			ExecuteError::Engine {
				diagnostic,
				..
			} => write!(f, "Engine error: {}", diagnostic.message),
		}
	}
}

impl std::error::Error for ExecuteError {}

impl From<StreamError> for ExecuteError {
	fn from(err: StreamError) -> Self {
		match err {
			StreamError::Query {
				diagnostic,
				statement,
			} => ExecuteError::Engine {
				diagnostic, // Preserve full diagnostic
				statement: statement.unwrap_or_default(),
			},
			StreamError::Cancelled => ExecuteError::Cancelled,
			StreamError::Timeout => ExecuteError::Timeout,
			StreamError::Disconnected => ExecuteError::Disconnected,
		}
	}
}

/// Result type for execute operations.
pub type ExecuteResult<T> = std::result::Result<T, ExecuteError>;

/// Execute a query with timeout.
///
/// This function:
/// 1. Starts the async query execution (internally uses spawn_blocking)
/// 2. Collects the stream with a timeout
/// 3. Returns the query results or an appropriate error
///
/// # Arguments
///
/// * `engine` - The database engine to execute the query on
/// * `query` - The RQL query string
/// * `identity` - The identity context for permission checking
/// * `params` - Query parameters
/// * `timeout` - Maximum time to wait for query completion
///
/// # Returns
///
/// * `Ok(Vec<Frame>)` - Query results on success
/// * `Err(ExecuteError::Timeout)` - If the query exceeds the timeout
/// * `Err(ExecuteError::Cancelled)` - If the query was cancelled
/// * `Err(ExecuteError::Engine)` - If the engine returns an error
///
/// # Example
///
/// ```ignore
/// let result = execute_query(
///     engine,
///     "SELECT * FROM users".to_string(),
///     identity,
///     Params::None,
///     Duration::from_secs(30),
/// ).await?;
/// ```
pub async fn execute_query(
	engine: StandardEngine,
	query: String,
	identity: Identity,
	params: Params,
	timeout: Duration,
) -> ExecuteResult<Vec<Frame>> {
	let stream = engine.query_as(&identity, &query, params);

	// Collect the stream with a timeout
	let result = tokio::time::timeout(timeout, stream.try_collect::<Vec<Frame>>()).await;

	match result {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(stream_result) => stream_result.map_err(ExecuteError::from),
	}
}

/// Execute a command with timeout.
///
/// Commands are write operations (INSERT, UPDATE, DELETE, DDL) that modify
/// the database state.
///
/// # Arguments
///
/// * `engine` - The database engine to execute the command on
/// * `statements` - The RQL command statements
/// * `identity` - The identity context for permission checking
/// * `params` - Command parameters
/// * `timeout` - Maximum time to wait for command completion
///
/// # Returns
///
/// * `Ok(Vec<Frame>)` - Command results on success
/// * `Err(ExecuteError::Timeout)` - If the command exceeds the timeout
/// * `Err(ExecuteError::Cancelled)` - If the command was cancelled
/// * `Err(ExecuteError::Engine)` - If the engine returns an error
pub async fn execute_command(
	engine: StandardEngine,
	statements: Vec<String>,
	identity: Identity,
	params: Params,
	timeout: Duration,
) -> ExecuteResult<Vec<Frame>> {
	let combined = statements.join("; ");
	let stream = engine.command_as(&identity, &combined, params);

	// Collect the stream with a timeout
	let result = tokio::time::timeout(timeout, stream.try_collect::<Vec<Frame>>()).await;

	match result {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(stream_result) => stream_result.map_err(ExecuteError::from),
	}
}

/// Execute a single query statement with timeout.
///
/// This is a convenience wrapper around `execute_query` for single statements.
pub async fn execute_query_single(
	engine: StandardEngine,
	query: &str,
	identity: Identity,
	params: Params,
	timeout: Duration,
) -> ExecuteResult<Vec<Frame>> {
	execute_query(engine, query.to_string(), identity, params, timeout).await
}

/// Execute a single command statement with timeout.
///
/// This is a convenience wrapper around `execute_command` for single statements.
pub async fn execute_command_single(
	engine: StandardEngine,
	command: &str,
	identity: Identity,
	params: Params,
	timeout: Duration,
) -> ExecuteResult<Vec<Frame>> {
	execute_command(engine, vec![command.to_string()], identity, params, timeout).await
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_execute_error_display() {
		let timeout_err = ExecuteError::Timeout;
		assert_eq!(timeout_err.to_string(), "Query execution timed out");

		let cancelled_err = ExecuteError::Cancelled;
		assert_eq!(cancelled_err.to_string(), "Query was cancelled");

		let disconnected_err = ExecuteError::Disconnected;
		assert_eq!(disconnected_err.to_string(), "Query stream disconnected unexpectedly");
	}
}
