// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{future::Future, pin::Pin, sync::Arc};

use reifydb_core::event::{
	EventBus,
	metric::{Request, RequestExecutedEvent},
};
use reifydb_metric::accumulator::StatementStatsAccumulator;
use reifydb_runtime::context::clock::Clock;
use reifydb_sub_server::{
	execute::ExecuteError,
	interceptor::{Operation, RequestContext, RequestInterceptor, ResponseContext},
};
use reifydb_type::value::datetime::DateTime;

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

			let request_record = match ctx.operation {
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

					Request::Query {
						fingerprint: ctx.metrics.request_fingerprint,
						statements: ctx.metrics.statements.clone(),
					}
				}
				Operation::Command => Request::Command {
					fingerprint: ctx.metrics.request_fingerprint,
					statements: ctx.metrics.statements.clone(),
				},
				Operation::Admin => Request::Admin {
					fingerprint: ctx.metrics.request_fingerprint,
					statements: ctx.metrics.statements.clone(),
				},
				Operation::Subscribe => return, // Ignore for request metrics
			};

			let timestamp = DateTime::from_timestamp_millis(clock.now_millis()).unwrap();
			event_bus.emit(RequestExecutedEvent::new(
				request_record,
				ctx.total,
				ctx.compute,
				success,
				timestamp,
			));
		})
	}
}
