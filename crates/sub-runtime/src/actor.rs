// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};
use reifydb_value::{
	params::Params,
	reifydb_assertions,
	value::{
		Value, datetime::DateTime, duration::Duration, identity::IdentityId, ordered_f64::OrderedF64,
		value_type::ValueType,
	},
};
use tracing::{debug, error};

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
	engine: StandardEngine,
	interval: Duration,
}

impl RuntimeSamplerActor {
	pub fn new(collectors: Collectors, engine: StandardEngine, interval: Duration) -> Self {
		Self {
			collectors,
			engine,
			interval,
		}
	}

	pub fn sample(&self, ts: DateTime) {
		let all = self.insert_domain_samples(ts);
		if all.is_empty() {
			return;
		}
		self.log_aggregate_metrics(&all);
	}

	#[inline]
	fn insert_domain_samples(&self, ts: DateTime) -> Vec<Sample> {
		let mut all: Vec<Sample> = Vec::new();
		for domain in Domain::ALL {
			let samples = domain.collect(&self.collectors);
			if samples.is_empty() {
				continue;
			}
			let rows = self.build_rows(ts, &samples);
			let mut builder = self.engine.bulk_insert_unchecked(IdentityId::system());
			builder.series(domain.snapshots_path()).rows(rows).done();
			if let Err(e) = builder.execute() {
				error!("runtime metrics insert into {} failed: {e}", domain.snapshots_path());
				continue;
			}
			all.extend(samples);
		}
		all
	}

	#[inline]
	fn build_rows(&self, ts: DateTime, samples: &[Sample]) -> Vec<Params> {
		samples.iter()
			.map(|s| {
				let mut map = HashMap::with_capacity(5);
				map.insert("ts".to_string(), Value::DateTime(ts));
				map.insert("scope".to_string(), Value::Utf8(s.scope.to_string()));
				map.insert("metric".to_string(), Value::Utf8(s.metric.to_string()));
				map.insert("value".to_string(), f64_value(s.value));
				map.insert("unit".to_string(), Value::Utf8(s.unit.to_string()));
				reifydb_assertions! {
					let len = map.len();
					assert!(
						len == 5,
						"row map was pre-sized with capacity(5) but ended with {len} entries; a \
						 mismatch means either a duplicate key silently dropped a metric column or \
						 a new column reallocates the map on every sample (defeats the pre-size)"
					);
				}
				Params::Named(Arc::new(map))
			})
			.collect()
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
			SamplerMessage::Tick(ts) => self.sample(ts),
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}

fn f64_value(v: f64) -> Value {
	OrderedF64::try_from(v).map(Value::Float8).unwrap_or(Value::None {
		inner: ValueType::Float8,
	})
}
