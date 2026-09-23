// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	execution::ExecutionResult,
	interface::catalog::{id::QueueId, token::Token},
	retry::RetryStrategy,
};
use reifydb_runtime::{context::clock::Instant, sync::waiter::WaiterHandle};
use reifydb_value::{
	params::Params,
	value::{Value, duration::Duration, identity::IdentityId},
};
use tracing::instrument;

use crate::{
	engine::StandardEngine,
	queue::{lookup::find_queue_id, wake::QueueWakeRegistry},
};

const CLAIM_RQL: &str = "CALL queue::claim($worker, $queue, $max_n, $lease_ttl)";

pub struct Session {
	engine: StandardEngine,
	identity: IdentityId,
	authenticated: bool,
	token: Option<String>,
	retry: RetryStrategy,
}

impl Session {
	pub fn from_token(engine: StandardEngine, info: &Token) -> Self {
		Self {
			engine,
			identity: info.identity,
			authenticated: true,
			token: None,
			retry: RetryStrategy::default(),
		}
	}

	pub fn from_token_with_value(engine: StandardEngine, info: &Token) -> Self {
		Self {
			engine,
			identity: info.identity,
			authenticated: true,
			token: Some(info.token.clone()),
			retry: RetryStrategy::default(),
		}
	}

	pub fn trusted(engine: StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			authenticated: false,
			token: None,
			retry: RetryStrategy::default(),
		}
	}

	pub fn anonymous(engine: StandardEngine) -> Self {
		Self::trusted(engine, IdentityId::anonymous())
	}

	pub fn with_retry(mut self, strategy: RetryStrategy) -> Self {
		self.retry = strategy;
		self
	}

	#[inline]
	pub fn identity(&self) -> IdentityId {
		self.identity
	}

	#[inline]
	pub fn token(&self) -> Option<&str> {
		self.token.as_deref()
	}

	#[inline]
	pub fn is_authenticated(&self) -> bool {
		self.authenticated
	}

	#[instrument(name = "session::query", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn query(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		self.engine.query_as(self.identity, rql, params.into())
	}

	#[instrument(name = "session::query_column", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn query_column(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		self.engine.query_column_as(self.identity, rql, params.into())
	}

	#[instrument(name = "session::command", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn command(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		let params = params.into();
		self.retry
			.execute(self.engine.rng(), rql, || self.engine.command_as(self.identity, rql, params.clone()))
	}

	#[instrument(name = "session::admin", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn admin(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		let params = params.into();
		self.retry.execute(self.engine.rng(), rql, || self.engine.admin_as(self.identity, rql, params.clone()))
	}

	#[instrument(name = "queue::claim_wait", level = "debug", skip(self), fields(queue = %queue, worker = %worker))]
	pub fn claim_wait(
		&self,
		queue: &str,
		worker: &str,
		max_n: u32,
		lease_ttl: Duration,
		wait_for: Duration,
	) -> ExecutionResult {
		let params = claim_params(queue, worker, max_n, lease_ttl);

		let mut result = self.command(CLAIM_RQL, params.clone());
		if !wait_for.is_positive() || result.error.is_some() || claimed_any(&result) {
			return result;
		}

		let Some(queue_id) = find_queue_id(&self.engine, self.identity, queue) else {
			return result;
		};
		let registry = self.engine.queue_wake();
		let clock = self.engine.clock();
		let deadline = clock.instant() + wait_for;

		loop {
			let waiter = Arc::new(WaiterHandle::on_clock(clock.clone()));
			let guard = ParkGuard::park(&registry, queue_id, waiter);

			result = self.command(CLAIM_RQL, params.clone());
			if result.error.is_some() || claimed_any(&result) {
				guard.forward_if_consumed();
				return result;
			}

			let Some(remaining) = remaining_budget(&clock.instant(), &deadline) else {
				return result;
			};
			guard.wait(remaining);
		}
	}
}

struct ParkGuard<'a> {
	registry: &'a QueueWakeRegistry,
	queue: QueueId,
	waiter: Arc<WaiterHandle>,
}

impl<'a> ParkGuard<'a> {
	fn park(registry: &'a QueueWakeRegistry, queue: QueueId, waiter: Arc<WaiterHandle>) -> Self {
		registry.register(queue, waiter.clone());
		Self {
			registry,
			queue,
			waiter,
		}
	}

	fn wait(&self, timeout: Duration) {
		self.waiter.wait_timeout(timeout);
	}

	fn forward_if_consumed(&self) {
		if self.waiter.wait_timeout(Duration::zero()) {
			self.registry.nudge(self.queue, 1);
		}
	}
}

impl Drop for ParkGuard<'_> {
	fn drop(&mut self) {
		self.registry.deregister(self.queue, &self.waiter);
	}
}

fn claim_params(queue: &str, worker: &str, max_n: u32, lease_ttl: Duration) -> Params {
	Params::Named(Arc::new(HashMap::from_iter([
		("worker".to_string(), Value::Utf8(worker.to_string())),
		("queue".to_string(), Value::Utf8(queue.to_string())),
		("max_n".to_string(), Value::Uint4(max_n)),
		("lease_ttl".to_string(), Value::Duration(lease_ttl)),
	])))
}

fn claimed_any(result: &ExecutionResult) -> bool {
	result.frames.iter().any(|frame| frame.row_count() > 0)
}

fn remaining_budget(now: &Instant, deadline: &Instant) -> Option<Duration> {
	if now >= deadline {
		return None;
	}
	Duration::from_nanoseconds(deadline.duration_since(now).as_nanos().min(i64::MAX as u128) as i64).ok()
}
