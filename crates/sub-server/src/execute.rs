// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Query and command execution with interceptor support.
//!
//! All execution goes through [`execute`], which runs pre/post interceptor
//! hooks around the actual engine dispatch. When no interceptors are
//! registered the overhead is a single `is_empty()` check.

use std::{error, fmt, sync::Arc, time::Duration};

use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_type::{
	error::{Diagnostic, Error},
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};
use tokio::time;
use tracing::warn;

use crate::interceptor::{Operation, RequestContext, RequestInterceptorChain, ResponseContext};

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

/// Retry a closure up to 3 times on `TXN_001` transaction conflict errors.
fn retry_on_conflict<F>(mut f: F) -> Result<Vec<Frame>, Error>
where
	F: FnMut() -> Result<Vec<Frame>, Error>,
{
	let mut last_err = None;
	for attempt in 0..3u32 {
		match f() {
			Ok(frames) => return Ok(frames),
			Err(err) if err.code == "TXN_001" => {
				warn!(attempt = attempt + 1, "Transaction conflict detected, retrying");
				last_err = Some(err);
			}
			Err(err) => return Err(err),
		}
	}
	Err(last_err.unwrap())
}

async fn raw_query(
	system: ActorSystem,
	engine: StandardEngine,
	query: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
	clock: Clock,
) -> ExecuteResult<(Vec<Frame>, Duration)> {
	let task = system.execute(move || {
		let t = clock.instant();
		let r = engine.query_as(identity, &query, params);
		(r, t.elapsed())
	});
	match time::timeout(timeout, task).await {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(Ok((result, compute))) => result.map(|f| (f, compute)).map_err(ExecuteError::from),
		Ok(Err(_join_error)) => Err(ExecuteError::Cancelled),
	}
}

async fn raw_command(
	system: ActorSystem,
	engine: StandardEngine,
	statements: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
	clock: Clock,
) -> ExecuteResult<(Vec<Frame>, Duration)> {
	let task = system.execute(move || {
		let t = clock.instant();
		let r = retry_on_conflict(|| engine.command_as(identity, &statements, params.clone()));
		(r, t.elapsed())
	});
	match time::timeout(timeout, task).await {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(Ok((result, compute))) => result.map(|f| (f, compute)).map_err(ExecuteError::from),
		Ok(Err(_join_error)) => Err(ExecuteError::Cancelled),
	}
}

async fn raw_admin(
	system: ActorSystem,
	engine: StandardEngine,
	statements: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
	clock: Clock,
) -> ExecuteResult<(Vec<Frame>, Duration)> {
	let task = system.execute(move || {
		let t = clock.instant();
		let r = retry_on_conflict(|| engine.admin_as(identity, &statements, params.clone()));
		(r, t.elapsed())
	});
	match time::timeout(timeout, task).await {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(Ok((result, compute))) => result.map(|f| (f, compute)).map_err(ExecuteError::from),
		Ok(Err(_join_error)) => Err(ExecuteError::Cancelled),
	}
}

async fn raw_subscription(
	system: ActorSystem,
	engine: StandardEngine,
	statement: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
	clock: Clock,
) -> ExecuteResult<(Vec<Frame>, Duration)> {
	let task = system.execute(move || {
		let t = clock.instant();
		let r = retry_on_conflict(|| engine.subscribe_as(identity, &statement, params.clone()));
		(r, t.elapsed())
	});
	match time::timeout(timeout, task).await {
		Err(_elapsed) => Err(ExecuteError::Timeout),
		Ok(Ok((result, compute))) => result.map(|f| (f, compute)).map_err(ExecuteError::from),
		Ok(Err(_join_error)) => Err(ExecuteError::Cancelled),
	}
}

/// Execute a database operation with interceptor support.
///
/// This is the single entry point for all protocol handlers.
/// Interceptors run before and after the engine dispatch:
///
/// 1. `pre_execute` — may reject the request or mutate identity/metadata
/// 2. Engine dispatch (query / command / admin / subscribe)
/// 3. `post_execute` — observes result and duration (fire-and-forget)
///
/// When the interceptor chain is empty, steps 1 and 3 are skipped.
pub async fn execute(
	chain: &RequestInterceptorChain,
	system: ActorSystem,
	engine: StandardEngine,
	mut ctx: RequestContext,
	timeout: Duration,
	clock: &Clock,
) -> ExecuteResult<(Vec<Frame>, Duration)> {
	// Pre-execute interceptors (may reject, may mutate identity)
	if !chain.is_empty() {
		chain.pre_execute(&mut ctx).await?;
	}

	let start = clock.instant();

	let operation = ctx.operation;
	let combined = ctx.statements.join("; ");

	// Clone params for response context only when interceptors need it
	let response_parts = if !chain.is_empty() {
		Some((ctx.identity, ctx.statements, ctx.params.clone(), ctx.metadata))
	} else {
		None
	};

	let result = match operation {
		Operation::Query => {
			raw_query(system, engine, combined, ctx.identity, ctx.params, timeout, clock.clone()).await
		}
		Operation::Command => {
			raw_command(system, engine, combined, ctx.identity, ctx.params, timeout, clock.clone()).await
		}
		Operation::Admin => {
			raw_admin(system, engine, combined, ctx.identity, ctx.params, timeout, clock.clone()).await
		}
		Operation::Subscribe => {
			raw_subscription(system, engine, combined, ctx.identity, ctx.params, timeout, clock.clone())
				.await
		}
	};

	let duration = start.elapsed();

	// Separate frames from compute_duration
	let (result, compute_duration) = match result {
		Ok((frames, cd)) => (Ok(frames), cd),
		Err(e) => (Err(e), duration),
	};

	// Post-execute interceptors
	if let Some((identity, statements, params, metadata)) = response_parts {
		let response_ctx = ResponseContext {
			identity,
			operation,
			statements,
			params,
			metadata,
			result: match &result {
				Ok(frames) => Ok(frames.len()),
				Err(e) => Err(e.to_string()),
			},
			duration,
			compute_duration,
		};
		chain.post_execute(&response_ctx).await;
	}

	result.map(|frames| (frames, duration))
}
