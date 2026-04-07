// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Query and command execution with interceptor support.
//!
//! All execution goes through [`execute`], which runs pre/post interceptor
//! hooks around the actual engine dispatch. When no interceptors are
//! registered the overhead is a single `is_empty()` check.

use std::{error, fmt, sync::Arc, time::Duration};

use reifydb_core::{execution::ExecutionResult, metric::ExecutionMetrics};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_type::{
	error::Diagnostic,
	params::Params,
	value::{duration::Duration as ReifyDuration, frame::frame::Frame, identity::IdentityId},
};
use tokio::{task::spawn_blocking, time};

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

/// Result type for execute operations.
pub type ExecuteResult<T> = Result<T, ExecuteError>;

/// Result of a raw engine dispatch: metrics are always present.
type RawResult = (ExecuteResult<Vec<Frame>>, ExecutionMetrics, Duration);

/// Convert an `ExecutionResult` into the raw result tuple.
fn result_to_raw(result: ExecutionResult) -> (Result<Vec<Frame>, ExecuteError>, ExecutionMetrics) {
	let metrics = result.metrics;
	match result.error {
		None => (Ok(result.frames), metrics),
		Some(err) => (
			Err(ExecuteError::Engine {
				diagnostic: Arc::new(err.diagnostic()),
				statement: String::new(),
			}),
			metrics,
		),
	}
}

async fn raw_query(
	engine: StandardEngine,
	query: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
	clock: Clock,
) -> RawResult {
	let task = spawn_blocking(move || -> (ExecutionResult, Duration) {
		let t = clock.instant();
		let r = engine.query_as(identity, &query, params);
		(r, t.elapsed())
	});
	match time::timeout(timeout, task).await {
		Err(_elapsed) => (Err(ExecuteError::Timeout), ExecutionMetrics::default(), Duration::ZERO),
		Ok(Ok((outcome, compute))) => {
			let (result, metrics) = result_to_raw(outcome);
			(result, metrics, compute)
		}
		Ok(Err(_join_error)) => (Err(ExecuteError::Cancelled), ExecutionMetrics::default(), Duration::ZERO),
	}
}

async fn raw_command(
	engine: StandardEngine,
	statements: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
	clock: Clock,
) -> RawResult {
	let task = spawn_blocking(move || -> (ExecutionResult, Duration) {
		let t = clock.instant();
		let r = engine.command_as(identity, &statements, params);
		(r, t.elapsed())
	});
	match time::timeout(timeout, task).await {
		Err(_elapsed) => (Err(ExecuteError::Timeout), ExecutionMetrics::default(), Duration::ZERO),
		Ok(Ok((outcome, compute))) => {
			let (result, metrics) = result_to_raw(outcome);
			(result, metrics, compute)
		}
		Ok(Err(_join_error)) => (Err(ExecuteError::Cancelled), ExecutionMetrics::default(), Duration::ZERO),
	}
}

async fn raw_admin(
	engine: StandardEngine,
	statements: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
	clock: Clock,
) -> RawResult {
	let task = spawn_blocking(move || -> (ExecutionResult, Duration) {
		let t = clock.instant();
		let r = engine.admin_as(identity, &statements, params);
		(r, t.elapsed())
	});
	match time::timeout(timeout, task).await {
		Err(_elapsed) => (Err(ExecuteError::Timeout), ExecutionMetrics::default(), Duration::ZERO),
		Ok(Ok((outcome, compute))) => {
			let (result, metrics) = result_to_raw(outcome);
			(result, metrics, compute)
		}
		Ok(Err(_join_error)) => (Err(ExecuteError::Cancelled), ExecutionMetrics::default(), Duration::ZERO),
	}
}

async fn raw_subscription(
	engine: StandardEngine,
	statement: String,
	identity: IdentityId,
	params: Params,
	timeout: Duration,
	clock: Clock,
) -> RawResult {
	let task = spawn_blocking(move || -> (ExecutionResult, Duration) {
		let t = clock.instant();
		let r = engine.subscribe_as(identity, &statement, params);
		(r, t.elapsed())
	});
	match time::timeout(timeout, task).await {
		Err(_elapsed) => (Err(ExecuteError::Timeout), ExecutionMetrics::default(), Duration::ZERO),
		Ok(Ok((outcome, compute))) => {
			let (result, metrics) = result_to_raw(outcome);
			(result, metrics, compute)
		}
		Ok(Err(_join_error)) => (Err(ExecuteError::Cancelled), ExecutionMetrics::default(), Duration::ZERO),
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

	let (result, metrics, compute_duration) = match operation {
		Operation::Query => raw_query(engine, combined, ctx.identity, ctx.params, timeout, clock.clone()).await,
		Operation::Command => {
			raw_command(engine, combined, ctx.identity, ctx.params, timeout, clock.clone()).await
		}
		Operation::Admin => raw_admin(engine, combined, ctx.identity, ctx.params, timeout, clock.clone()).await,
		Operation::Subscribe => {
			raw_subscription(engine, combined, ctx.identity, ctx.params, timeout, clock.clone()).await
		}
	};

	let duration = start.elapsed();

	// Post-execute interceptors
	if let Some((identity, statements, params, metadata)) = response_parts {
		let total = ReifyDuration::from_microseconds(duration.as_micros() as i64).unwrap();
		let compute = ReifyDuration::from_microseconds(compute_duration.as_micros() as i64).unwrap();
		let response_ctx = ResponseContext {
			identity,
			operation,
			statements,
			metrics,
			params,
			metadata,
			result: match &result {
				Ok(frames) => Ok(frames.len()),
				Err(e) => Err(e.to_string()),
			},
			total,
			compute,
		};
		chain.post_execute(&response_ctx).await;
	}

	result.map(|frames| (frames, duration))
}
