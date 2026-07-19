// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_runtime::{
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::framework::{current::CurrentCache, source::MetricsSource};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshDomain {
	RuntimeMemory,
	RuntimeWatermarks,
	RuntimeOperators,
	ReadBuffer,
	Instruments,
}

impl RefreshDomain {
	pub const ALL: [RefreshDomain; 5] = [
		RefreshDomain::RuntimeMemory,
		RefreshDomain::RuntimeWatermarks,
		RefreshDomain::RuntimeOperators,
		RefreshDomain::ReadBuffer,
		RefreshDomain::Instruments,
	];

	pub fn config_key(&self) -> ConfigKey {
		match self {
			RefreshDomain::RuntimeMemory => ConfigKey::MetricsRuntimeMemoryRefreshInterval,
			RefreshDomain::RuntimeWatermarks => ConfigKey::MetricsRuntimeWatermarksRefreshInterval,
			RefreshDomain::RuntimeOperators => ConfigKey::MetricsRuntimeOperatorsRefreshInterval,
			RefreshDomain::ReadBuffer => ConfigKey::MetricsReadBufferRefreshInterval,
			RefreshDomain::Instruments => ConfigKey::MetricsInstrumentsRefreshInterval,
		}
	}

	pub fn actor_name(&self) -> &'static str {
		match self {
			RefreshDomain::RuntimeMemory => "metrics-refresh-memory",
			RefreshDomain::RuntimeWatermarks => "metrics-refresh-watermarks",
			RefreshDomain::RuntimeOperators => "metrics-refresh-operators",
			RefreshDomain::ReadBuffer => "metrics-refresh-read-buffer",
			RefreshDomain::Instruments => "metrics-refresh-instruments",
		}
	}
}

#[derive(Clone, Debug)]
pub enum RefreshMessage {
	Tick,
}

pub struct RefreshActor {
	targets: Vec<(Arc<dyn MetricsSource>, CurrentCache)>,
	clock: Clock,
	interval: Duration,
}

impl RefreshActor {
	pub fn new(targets: Vec<(Arc<dyn MetricsSource>, CurrentCache)>, clock: Clock, interval: Duration) -> Self {
		Self {
			targets,
			clock,
			interval,
		}
	}

	fn refresh(&self) {
		let now = DateTime::from_nanos(self.clock.now_nanos());
		for (source, cache) in &self.targets {
			cache.store(source.collect(now));
		}
	}
}

impl Actor for RefreshActor {
	type Message = RefreshMessage;
	type State = ();

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.interval, || RefreshMessage::Tick);
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			RefreshMessage::Tick => {
				self.refresh();
				ctx.schedule_once(self.interval, || RefreshMessage::Tick);
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}
