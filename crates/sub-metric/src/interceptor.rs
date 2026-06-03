// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{future::Future, pin::Pin, sync::Arc};

use reifydb_core::{
	actors::server::Operation,
	event::{
		EventBus,
		metric::{Request, RequestExecutedEvent},
	},
};
use reifydb_metric::accumulator::StatementStatsAccumulator;
use reifydb_runtime::context::clock::Clock;
use reifydb_sub_server::{
	execute::ExecuteError,
	interceptor::{RequestContext, RequestInterceptor, ResponseContext},
};
use reifydb_value::{reifydb_assertions, value::datetime::DateTime};

pub struct RequestMetricsInterceptor {
	event_bus: EventBus,
	accumulator: Arc<StatementStatsAccumulator>,
	clock: Clock,
}

impl RequestMetricsInterceptor {
	pub fn new(event_bus: EventBus, accumulator: Arc<StatementStatsAccumulator>, clock: Clock) -> Self {
		Self {
			event_bus,
			accumulator,
			clock,
		}
	}

	#[inline]
	fn build_request_record(
		accumulator: &StatementStatsAccumulator,
		ctx: &ResponseContext,
		success: bool,
	) -> Option<Request> {
		match ctx.operation {
			Operation::Query => {
				for stmt in &ctx.metrics.statements {
					accumulator.record(
						stmt.fingerprint,
						&stmt.normalized_rql,
						stmt.execute_duration_us,
						stmt.compile_duration_us,
						stmt.rows_affected,
						success,
					);
				}

				Some(Request::Query {
					fingerprint: ctx.metrics.fingerprint,
					statements: ctx.metrics.statements.clone(),
				})
			}
			Operation::Command => Some(Request::Command {
				fingerprint: ctx.metrics.fingerprint,
				statements: ctx.metrics.statements.clone(),
			}),
			Operation::Admin => Some(Request::Admin {
				fingerprint: ctx.metrics.fingerprint,
				statements: ctx.metrics.statements.clone(),
			}),
			Operation::Subscribe => None,
		}
	}
}

impl RequestInterceptor for RequestMetricsInterceptor {
	fn pre_execute<'a>(
		&'a self,
		_ctx: &'a mut RequestContext,
	) -> Pin<Box<dyn Future<Output = Result<(), ExecuteError>> + Send + 'a>> {
		Box::pin(async { Ok(()) })
	}

	fn post_execute<'a>(&'a self, ctx: &'a ResponseContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
		let event_bus = self.event_bus.clone();
		let accumulator = self.accumulator.clone();
		let clock = self.clock.clone();

		Box::pin(async move {
			let success = ctx.result.is_ok();

			let Some(request_record) = Self::build_request_record(&accumulator, ctx, success) else {
				reifydb_assertions! {
					assert!(
						matches!(ctx.operation, Operation::Subscribe),
						"build_request_record returned None for a metered operation {:?}; only Subscribe \
						 requests are exempt from RequestExecutedEvent, so a None here silently drops \
						 metrics for a request that should have been recorded",
						ctx.operation
					);
				}
				return;
			};

			let timestamp = DateTime::from_timestamp_millis(clock.now_millis()).unwrap();
			event_bus.emit(RequestExecutedEvent::new(
				request_record,
				ctx.metrics.total,
				ctx.metrics.compute,
				success,
				timestamp,
			));
		})
	}
}
