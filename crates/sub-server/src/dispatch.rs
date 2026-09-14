// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(reifydb_single_threaded))]
pub use host::{dispatch, dispatch_subscribe};

#[cfg(not(reifydb_single_threaded))]
mod host {
	use std::sync::Arc;

	use reifydb_core::{
		actors::server::{ServerMessage, ServerResponse, ServerSubscribeResponse, build_server_message},
		interface::catalog::subscription::{SubscribeOptions, SubscribeOutcome},
		metrics::execution::ExecutionMetrics,
	};
	use reifydb_runtime::{actor::reply::reply_channel, context::clock::Instant};
	use reifydb_value::{
		error::{IntoDiagnostic, TypeError},
		value::{duration::Duration, frame::frame::Frame},
	};
	use tokio::time::timeout;
	use tracing::instrument;

	use crate::{
		execute::ExecuteError,
		interceptor::{RequestContext, ResponseContext},
		state::AppState,
	};

	#[instrument(name = "dispatch::request", level = "debug", skip_all, fields(op = ?ctx.operation))]
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

	#[instrument(name = "dispatch::subscribe", level = "debug", skip_all, fields(op = ?ctx.operation))]
	pub async fn dispatch_subscribe(
		state: &AppState,
		mut ctx: RequestContext,
		options: SubscribeOptions,
	) -> Result<SubscribeOutcome, ExecuteError> {
		run_pre_execute(state, &mut ctx).await?;
		let start = state.clock().instant();
		let response = send_subscribe_message(state, &ctx, options).await?;
		let (outcome, metrics) = finalize_subscribe_metrics(response, start)?;
		run_post_execute(state, &ctx, &metrics, 0).await;
		Ok(outcome)
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
		timeout(state.query_timeout().to_std(), receiver.recv())
			.await
			.map_err(|_| ExecuteError::Timeout)?
			.map_err(|_| ExecuteError::Disconnected)
	}

	#[instrument(name = "dispatch::send_subscribe_message", level = "debug", skip_all)]
	async fn send_subscribe_message(
		state: &AppState,
		ctx: &RequestContext,
		options: SubscribeOptions,
	) -> Result<ServerSubscribeResponse, ExecuteError> {
		let (reply, receiver) = reply_channel();
		let msg = ServerMessage::Subscribe {
			identity: ctx.identity,
			rql: ctx.rql.clone(),
			params: ctx.params.clone(),
			options,
			reply,
		};
		let (actor_ref, _handle) = state.spawn_server_actor();
		actor_ref.send(msg).ok().ok_or(ExecuteError::Disconnected)?;
		timeout(state.query_timeout().to_std(), receiver.recv())
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
		metrics.total =
			Duration::from_nanoseconds(wall_duration.as_nanos() as i64).map_err(|e| duration_error(*e))?;
		metrics.compute = Duration::from_nanoseconds(compute_duration.to_std().as_nanos() as i64)
			.map_err(|e| duration_error(*e))?;
		Ok((frames, metrics))
	}

	#[inline]
	fn finalize_subscribe_metrics(
		response: ServerSubscribeResponse,
		start: Instant,
	) -> Result<(SubscribeOutcome, ExecutionMetrics), ExecuteError> {
		let wall_duration = start.elapsed();
		let (outcome, compute_duration) = match response {
			ServerSubscribeResponse::Subscribed {
				outcome,
				duration,
			} => (outcome, duration),
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
		let metrics = ExecutionMetrics {
			total: Duration::from_nanoseconds(wall_duration.as_nanos() as i64)
				.map_err(|e| duration_error(*e))?,
			compute: Duration::from_nanoseconds(compute_duration.to_std().as_nanos() as i64)
				.map_err(|e| duration_error(*e))?,
			..Default::default()
		};
		Ok((outcome, metrics))
	}

	fn duration_error(error: TypeError) -> ExecuteError {
		ExecuteError::Engine {
			diagnostic: Arc::from(error.into_diagnostic()),
			rql: String::new(),
		}
	}
}
