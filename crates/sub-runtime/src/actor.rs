// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};
use reifydb_value::value::{datetime::DateTime, duration::Duration};
use tracing::debug;

use crate::{
	collect::{Collectors, Sample},
	domain::Domain,
};

#[derive(Clone, Debug)]
pub enum SamplerMessage {
	Tick(DateTime),
}

pub struct RuntimeSamplerActor {
	collectors: Collectors,
	interval: Duration,
}

impl RuntimeSamplerActor {
	pub fn new(collectors: Collectors, interval: Duration) -> Self {
		Self {
			collectors,
			interval,
		}
	}

	pub fn sample(&self) {
		let mut all: Vec<Sample> = Vec::new();
		for domain in Domain::ALL {
			all.extend(domain.collect(&self.collectors));
		}
		if all.is_empty() {
			return;
		}
		self.log_aggregate_metrics(&all);
	}

	#[inline]
	fn log_aggregate_metrics(&self, all: &[Sample]) {
		let get = |name: &str| all.iter().find(|s| s.metric == name).map(|s| s.value).unwrap_or(0.0);
		debug!(
			rss_anon_mb = get("rss_anon_bytes") / 1_048_576.0,
			rss_file_mb = get("rss_file_bytes") / 1_048_576.0,
			jemalloc_allocated_mb = get("jemalloc_allocated_bytes") / 1_048_576.0,
			jemalloc_resident_mb = get("jemalloc_resident_bytes") / 1_048_576.0,
			dict_reservations = get("reservation_count"),
			watermark_lag = get("watermark_lag"),
			oracle_windows = get("oracle_window_count"),
			buffer_keys = get("buffer_current_keys_total"),
			"runtime metrics"
		);
	}
}

impl Actor for RuntimeSamplerActor {
	type Message = SamplerMessage;
	type State = ();

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_tick(self.interval, |nanos| SamplerMessage::Tick(DateTime::from_nanos(nanos)));
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			SamplerMessage::Tick(_) => self.sample(),
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}
