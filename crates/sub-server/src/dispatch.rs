// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Shared dispatch layer for all network transports.
//!
//! Pure helpers (`build_server_message`) live in `reifydb_core::actors::server`
//! so both native transports and DST clients can share them.
//! The async `dispatch()` / `dispatch_subscribe()` functions are the single
//! entry points for native transport handlers.

#[cfg(not(reifydb_single_threaded))]
pub use native::{dispatch, dispatch_subscribe};

#[cfg(not(reifydb_single_threaded))]
mod native {
	use std::sync::Arc;

	use reifydb_core::{
		actors::server::{ServerMessage, ServerResponse, ServerSubscribeResponse, build_server_message},
		metric::ExecutionMetrics,
	};
	use reifydb_runtime::actor::reply::reply_channel;
	use reifydb_type::value::{duration::Duration as ReifyDuration, frame::frame::Frame};
	use tokio::time::timeout;

	use crate::{
		execute::ExecuteError,
		interceptor::{RequestContext, ResponseContext},
		state::AppState,
	};

	/// Dispatch a query/command/admin operation through the actor with interceptors.
	///
	/// This is the single entry point for all native transport handlers.
	/// The caller is responsible only for:
	/// 1. Extracting identity from transport-specific auth
	/// 2. Building `RequestMetadata` from transport-specific headers
	/// 3. Parsing params from transport-specific wire format
	/// 4. Converting the result into transport-specific response format
	pub async fn dispatch(
		state: &AppState,
		mut ctx: RequestContext,
	) -> Result<(Vec<Frame>, ExecutionMetrics), ExecuteError> {
		// Pre-execute interceptors
		if !state.request_interceptors().is_empty() {
			state.request_interceptors().pre_execute(&mut ctx).await?;
		}

		let start = state.clock().instant();

		// Build message and send to per-request actor
		let (reply, receiver) = reply_channel();
		let msg = build_server_message(ctx.operation, ctx.identity, ctx.rql.clone(), ctx.params.clone(), reply);

		let (actor_ref, _handle) = state.spawn_server_actor();
		actor_ref.send(msg).ok().ok_or(ExecuteError::Disconnected)?;

		// Await reply with timeout
		let server_response = timeout(state.query_timeout(), receiver.recv())
			.await
			.map_err(|_| ExecuteError::Timeout)?
			.map_err(|_| ExecuteError::Disconnected)?;

		let wall_duration = start.elapsed();
		let (frames, compute_duration, mut metrics) = match server_response {
			ServerResponse::Success {
				frames,
				duration,
				metrics,
			} => Ok((frames, duration, metrics)),
			ServerResponse::EngineError {
				diagnostic,
				rql,
			} => Err(ExecuteError::Engine {
				diagnostic: Arc::from(diagnostic),
				rql,
			}),
		}?;

		metrics.total = ReifyDuration::from_nanoseconds(wall_duration.as_nanos() as i64).unwrap_or_default();
		metrics.compute =
			ReifyDuration::from_nanoseconds(compute_duration.as_nanos() as i64).unwrap_or_default();

		// Post-execute interceptors
		if !state.request_interceptors().is_empty() {
			let response_ctx = ResponseContext {
				identity: ctx.identity,
				operation: ctx.operation,
				rql: ctx.rql,
				params: ctx.params,
				metadata: ctx.metadata,
				metrics: metrics.clone(),
				result: Ok(frames.len()),
			};
			state.request_interceptors().post_execute(&response_ctx).await;
		}

		Ok((frames, metrics))
	}

	/// Dispatch a subscribe operation through the actor with interceptors.
	///
	/// Separate from `dispatch()` because Subscribe uses `Reply<ServerSubscribeResponse>`
	/// rather than `Reply<ServerResponse>`.
	pub async fn dispatch_subscribe(
		state: &AppState,
		mut ctx: RequestContext,
	) -> Result<(Vec<Frame>, ExecutionMetrics), ExecuteError> {
		// Pre-execute interceptors
		if !state.request_interceptors().is_empty() {
			state.request_interceptors().pre_execute(&mut ctx).await?;
		}

		let start = state.clock().instant();

		let (reply, receiver) = reply_channel();
		let msg = ServerMessage::Subscribe {
			identity: ctx.identity,
			rql: ctx.rql.clone(),
			reply,
		};

		let (actor_ref, _handle) = state.spawn_server_actor();
		actor_ref.send(msg).ok().ok_or(ExecuteError::Disconnected)?;

		let response = timeout(state.query_timeout(), receiver.recv())
			.await
			.map_err(|_| ExecuteError::Timeout)?
			.map_err(|_| ExecuteError::Disconnected)?;

		let wall_duration = start.elapsed();

		let (frames, compute_duration, mut metrics) = match response {
			ServerSubscribeResponse::Subscribed {
				frames,
				duration,
				metrics,
			} => (frames, duration, metrics),
			ServerSubscribeResponse::EngineError {
				diagnostic,
				rql,
			} => {
				return Err(ExecuteError::Engine {
					diagnostic: Arc::from(diagnostic),
					rql,
				});
			}
		};

		metrics.total = ReifyDuration::from_nanoseconds(wall_duration.as_nanos() as i64).unwrap_or_default();
		metrics.compute =
			ReifyDuration::from_nanoseconds(compute_duration.as_nanos() as i64).unwrap_or_default();

		// Post-execute interceptors
		if !state.request_interceptors().is_empty() {
			let response_ctx = ResponseContext {
				identity: ctx.identity,
				operation: ctx.operation,
				rql: ctx.rql,
				params: ctx.params,
				metadata: ctx.metadata,
				metrics: metrics.clone(),
				result: Ok(frames.len()),
			};
			state.request_interceptors().post_execute(&response_ctx).await;
		}

		Ok((frames, metrics))
	}
}
