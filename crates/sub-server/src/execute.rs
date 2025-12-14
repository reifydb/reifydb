// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Query and command execution with async/sync bridging.
//!
//! This module provides async wrappers around the synchronous database engine
//! operations using `spawn_blocking` to avoid blocking tokio worker threads.
//!
//! All database operations are executed on tokio's blocking thread pool with
//! configurable timeouts.

use std::time::Duration;

use reifydb_core::{
	Frame,
	interface::{Engine, Identity},
};
use reifydb_engine::StandardEngine;
use reifydb_type::Params;
use tokio::task::spawn_blocking;

/// Error types for query/command execution.
#[derive(Debug)]
pub enum ExecuteError {
	/// Query exceeded the configured timeout.
	Timeout,
	/// The blocking task panicked during execution.
	TaskPanic(String),
	/// Database engine returned an error, with the statement that caused it.
	Engine {
		error: reifydb_type::Error,
		statement: String,
	},
}

impl std::fmt::Display for ExecuteError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ExecuteError::Timeout => write!(f, "Query execution timed out"),
			ExecuteError::TaskPanic(msg) => write!(f, "Query task panicked: {}", msg),
			ExecuteError::Engine {
				error,
				..
			} => write!(f, "Engine error: {}", error),
		}
	}
}

impl std::error::Error for ExecuteError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			ExecuteError::Engine {
				error,
				..
			} => Some(error),
			_ => None,
		}
	}
}

/// Result type for execute operations.
pub type ExecuteResult<T> = std::result::Result<T, ExecuteError>;

/// Execute a query on the blocking thread pool with timeout.
///
/// This function:
/// 1. Spawns the query execution on tokio's blocking thread pool
/// 2. Applies a timeout to prevent stuck queries from hanging indefinitely
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
/// * `Err(ExecuteError::TaskPanic)` - If the blocking task panics
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
	let query_clone = query.clone();
	let result =
		tokio::time::timeout(timeout, spawn_blocking(move || engine.query_as(&identity, &query, params))).await;

	match result {
		// Timeout expired
		Err(_elapsed) => Err(ExecuteError::Timeout),
		// spawn_blocking returned
		Ok(join_result) => match join_result {
			// Task panicked
			Err(join_err) => Err(ExecuteError::TaskPanic(join_err.to_string())),
			// Task completed
			Ok(engine_result) => engine_result.map_err(|e| ExecuteError::Engine {
				error: e,
				statement: query_clone,
			}),
		},
	}
}

/// Execute a command on the blocking thread pool with timeout.
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
/// * `Err(ExecuteError::TaskPanic)` - If the blocking task panics
/// * `Err(ExecuteError::Engine)` - If the engine returns an error
pub async fn execute_command(
	engine: StandardEngine,
	statements: Vec<String>,
	identity: Identity,
	params: Params,
	timeout: Duration,
) -> ExecuteResult<Vec<Frame>> {
	let combined = statements.join("; ");
	let combined_clone = combined.clone();
	let result =
		tokio::time::timeout(timeout, spawn_blocking(move || engine.command_as(&identity, &combined, params)))
			.await;

	match result {
		// Timeout expired
		Err(_elapsed) => Err(ExecuteError::Timeout),
		// spawn_blocking returned
		Ok(join_result) => match join_result {
			// Task panicked
			Err(join_err) => Err(ExecuteError::TaskPanic(join_err.to_string())),
			// Task completed
			Ok(engine_result) => engine_result.map_err(|e| ExecuteError::Engine {
				error: e,
				statement: combined_clone,
			}),
		},
	}
}

/// Execute a single query statement on the blocking thread pool with timeout.
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

/// Execute a single command statement on the blocking thread pool with timeout.
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

		let panic_err = ExecuteError::TaskPanic("test panic".to_string());
		assert_eq!(panic_err.to_string(), "Query task panicked: test panic");
	}
}
