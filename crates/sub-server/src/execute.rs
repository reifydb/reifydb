// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Query and command execution with async streaming.
//!
//! This module provides async wrappers around the database engine operations.
//! The engine uses a compute pool for sync execution, streaming results back
//! through async channels.

use std::{error, fmt, sync::Arc, time::Duration};

use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::system::ActorSystem;
use reifydb_type::{
	error::{Diagnostic, Error},
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};
use tokio::time;

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
		}
	}
}

impl error::Error for ExecuteError {}

impl From<Error> for ExecuteError {
	fn from(err: Error) -> Self {
		ExecuteError::Engine {
			diagnostic: Arc::new(err.diagnostic()),
			statement: String::new(),
		}
	}
}

/// Result type for execute operations.
pub type ExecuteResult<T> = Result<T, ExecuteError>;

/// Execute a query with timeout.
///
/// This function:
/// 1. Starts the query execution on the actor system's compute pool
/// 2. Applies a timeout to the operation
/// 3. Returns the query results or an appropriate error
///
/// # Arguments
///
/// * `system` - The actor system to execute the query on
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
///     system,
///     engine,
///     "FROM users take 42".to_string(),
///     identity,
///     Params::None,
///     Duration::from_secs(30),
/// ).await?;
/// ```
pub async fn execute_query(
	system: ActorSystem,
	engine: StandardEngine,
	query: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
) -> ExecuteResult<Vec<Frame>> {
	// Execute synchronous query on actor system's compute pool with timeout
	let task = system.compute(move || engine.query_as(identity, &query, params));

	let result = time::timeout(timeout, task).await;

	match result {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(Ok(frames_result)) => frames_result.map_err(ExecuteError::from),
		Ok(Err(_join_error)) => Err(ExecuteError::Cancelled),
	}
}

/// Execute an admin operation with timeout.
///
/// Admin operations include DDL (CREATE TABLE, ALTER, etc.), DML (INSERT, UPDATE, DELETE),
/// and read queries. This is the most privileged execution level.
///
/// # Arguments
///
/// * `system` - The actor system to execute the admin operation on
/// * `engine` - The database engine to execute the admin operation on
/// * `statements` - The RQL admin statements
/// * `identity` - The identity context for permission checking
/// * `params` - Admin parameters
/// * `timeout` - Maximum time to wait for admin completion
///
/// # Returns
///
/// * `Ok(Vec<Frame>)` - Admin results on success
/// * `Err(ExecuteError::Timeout)` - If the admin operation exceeds the timeout
/// * `Err(ExecuteError::Cancelled)` - If the admin operation was cancelled
/// * `Err(ExecuteError::Engine)` - If the engine returns an error
pub async fn execute_admin(
	system: ActorSystem,
	engine: StandardEngine,
	statements: Vec<String>,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
) -> ExecuteResult<Vec<Frame>> {
	let combined = statements.join("; ");

	// Execute synchronous admin operation on actor system's compute pool with timeout
	let task = system.compute(move || engine.admin_as(identity, &combined, params));

	let result = time::timeout(timeout, task).await;

	match result {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(Ok(frames_result)) => frames_result.map_err(ExecuteError::from),
		Ok(Err(_join_error)) => Err(ExecuteError::Cancelled),
	}
}

/// Execute a command with timeout.
///
/// Commands are write operations (INSERT, UPDATE, DELETE) that modify
/// the database state. DDL operations are not allowed in command transactions.
///
/// # Arguments
///
/// * `system` - The actor system to execute the command on
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
	system: ActorSystem,
	engine: StandardEngine,
	statements: Vec<String>,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
) -> ExecuteResult<Vec<Frame>> {
	let combined = statements.join("; ");

	// Execute synchronous command on actor system's compute pool with timeout
	let task = system.compute(move || engine.command_as(identity, &combined, params));

	let result = time::timeout(timeout, task).await;

	match result {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(Ok(frames_result)) => frames_result.map_err(ExecuteError::from),
		Ok(Err(_join_error)) => Err(ExecuteError::Cancelled),
	}
}
