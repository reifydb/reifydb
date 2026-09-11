// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{Debug, Display},
	future::Future,
};

use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::context::{clock::Clock, rng::Rng};
use reifydb_value::{
	params::Params,
	value::{duration::Duration, frame::frame::Frame, identity::IdentityId},
};

#[derive(Clone)]
pub struct SubscribeContext {
	engine: StandardEngine,
	clock: Clock,
	rng: Rng,
	max_hydration_rows: u64,
	min_throttle: Duration,
	min_linger: Duration,
}

impl SubscribeContext {
	pub fn new(
		engine: StandardEngine,
		clock: Clock,
		rng: Rng,
		max_hydration_rows: u64,
		min_throttle: Duration,
		min_linger: Duration,
	) -> Self {
		Self {
			engine,
			clock,
			rng,
			max_hydration_rows,
			min_throttle,
			min_linger,
		}
	}

	#[inline]
	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	#[inline]
	pub fn engine_clone(&self) -> StandardEngine {
		self.engine.clone()
	}

	#[inline]
	pub fn clock(&self) -> &Clock {
		&self.clock
	}

	#[inline]
	pub fn rng(&self) -> &Rng {
		&self.rng
	}

	#[inline]
	pub fn max_hydration_rows(&self) -> u64 {
		self.max_hydration_rows
	}

	#[inline]
	pub fn min_throttle(&self) -> Duration {
		self.min_throttle
	}

	#[inline]
	pub fn min_linger(&self) -> Duration {
		self.min_linger
	}

	pub fn clamp_throttle(&self, requested: Option<Duration>) -> Duration {
		match requested {
			Some(d) if !d.is_zero() => d.max(self.min_throttle),
			_ => Duration::zero(),
		}
	}

	pub fn clamp_linger(&self, requested: Option<Duration>) -> Duration {
		match requested {
			Some(d) if !d.is_zero() => d.max(self.min_linger),
			_ => Duration::zero(),
		}
	}
}

pub trait SubscribeHost: Send + Sync {
	type Error: Debug + Display + Send + Sync + 'static;

	fn context(&self) -> &SubscribeContext;

	fn execute_subscribe(
		&self,
		identity: IdentityId,
		rql: String,
		params: Params,
	) -> impl Future<Output = Result<Vec<Frame>, Self::Error>> + Send;
}
