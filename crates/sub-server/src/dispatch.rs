// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[cfg(not(reifydb_single_threaded))]
pub use native::{dispatch, dispatch_subscribe};

#[cfg(not(reifydb_single_threaded))]
mod native {
	use std::sync::Arc;

	use reifydb_core::{
		actors::server::{ServerMessage, ServerResponse, ServerSubscribeResponse, build_server_message},
		metric::ExecutionMetrics,
	};
	use reifydb_runtime::{actor::reply::reply_channel, context::clock::Instant};
	use reifydb_type::value::{duration::Duration as ReifyDuration, frame::frame::Frame};
	use tokio::time::timeout;
	use tracing::instrument;

	use crate::{
		execute::ExecuteError,
		interceptor::{RequestContext, ResponseContext},
		state::AppState,
	};

	#[instrument(name = "dispatch", level = "debug", skip_all, fields(op = ?ctx.operation))]
	pub async fn dispatch(
		state: &AppState,
		mut ctx: RequestContext,
	) -> Result<(Vec<Frame>, ExecutionMetrics), ExecuteError> {
		run_pre_execute(state, &mut ctx).await?;
		let start = state.clock().instant();
		let response = send_server_message(state, &ctx).await?;
		let (frames, metrics) = finalize_dispatch_metrics(response, start)?;
		run_post_execute(state, &ctx, &metrics, frames.len()).await;
		Ok((frames, metrics))
	}

	#[instrument(name = "dispatch_subscribe", level = "debug", skip_all, fields(op = ?ctx.operation))]
	pub async fn dispatch_subscribe(
		state: &AppState,
		mut ctx: RequestContext,
	) -> Result<(Vec<Frame>, ExecutionMetrics), ExecuteError> {
		run_pre_execute(state, &mut ctx).await?;
		let start = state.clock().instant();
		let response = send_subscribe_message(state, &ctx).await?;
		let (frames, metrics) = finalize_subscribe_metrics(response, start)?;
		run_post_execute(state, &ctx, &metrics, frames.len()).await;
		Ok((frames, metrics))
	}

	#[inline]
	async fn run_pre_execute(state: &AppState, ctx: &mut RequestContext) -> Result<(), ExecuteError> {
		if !state.request_interceptors().is_empty() {
			state.request_interceptors().pre_execute(ctx).await?;
		}
		Ok(())
	}

	#[inline]
	async fn run_post_execute(
		state: &AppState,
		ctx: &RequestContext,
		metrics: &ExecutionMetrics,
		frame_count: usize,
	) {
		if state.request_interceptors().is_empty() {
			return;
		}
		let response_ctx = ResponseContext {
			identity: ctx.identity,
			operation: ctx.operation,
			rql: ctx.rql.clone(),
			params: ctx.params.clone(),
			metadata: ctx.metadata.clone(),
			metrics: metrics.clone(),
			result: Ok(frame_count),
		};
		state.request_interceptors().post_execute(&response_ctx).await;
	}

	#[instrument(name = "dispatch::send_server_message", level = "debug", skip_all)]
	async fn send_server_message(state: &AppState, ctx: &RequestContext) -> Result<ServerResponse, ExecuteError> {
		let (reply, receiver) = reply_channel();
		let msg = build_server_message(ctx.operation, ctx.identity, ctx.rql.clone(), ctx.params.clone(), reply);
		let (actor_ref, _handle) = state.spawn_server_actor();
		actor_ref.send(msg).ok().ok_or(ExecuteError::Disconnected)?;
		timeout(state.query_timeout(), receiver.recv())
			.await
			.map_err(|_| ExecuteError::Timeout)?
			.map_err(|_| ExecuteError::Disconnected)
	}

	#[instrument(name = "dispatch::send_subscribe_message", level = "debug", skip_all)]
	async fn send_subscribe_message(
		state: &AppState,
		ctx: &RequestContext,
	) -> Result<ServerSubscribeResponse, ExecuteError> {
		let (reply, receiver) = reply_channel();
		let msg = ServerMessage::Subscribe {
			identity: ctx.identity,
			rql: ctx.rql.clone(),
			reply,
		};
		let (actor_ref, _handle) = state.spawn_server_actor();
		actor_ref.send(msg).ok().ok_or(ExecuteError::Disconnected)?;
		timeout(state.query_timeout(), receiver.recv())
			.await
			.map_err(|_| ExecuteError::Timeout)?
			.map_err(|_| ExecuteError::Disconnected)
	}

	#[inline]
	fn finalize_dispatch_metrics(
		response: ServerResponse,
		start: Instant,
	) -> Result<(Vec<Frame>, ExecutionMetrics), ExecuteError> {
		let wall_duration = start.elapsed();
		let (frames, compute_duration, mut metrics) = match response {
			ServerResponse::Success {
				frames,
				duration,
				metrics,
			} => (frames, duration, metrics),
			ServerResponse::EngineError {
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
		Ok((frames, metrics))
	}

	#[inline]
	fn finalize_subscribe_metrics(
		response: ServerSubscribeResponse,
		start: Instant,
	) -> Result<(Vec<Frame>, ExecutionMetrics), ExecuteError> {
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
		Ok((frames, metrics))
	}
}
