// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc, time::Duration as StdDuration};

use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};
use reifydb_value::{
	params::Params,
	value::{Value, datetime::DateTime, identity::IdentityId, ordered_f64::OrderedF64, value_type::ValueType},
};
use tracing::{error, info};

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
	interval: StdDuration,
}

impl RuntimeSamplerActor {
	pub fn new(collectors: Collectors, engine: StandardEngine, interval: StdDuration) -> Self {
		Self {
			collectors,
			engine,
			interval,
		}
	}

	pub fn sample(&self, ts: DateTime) {
		let mut all: Vec<Sample> = Vec::new();

		for domain in Domain::ALL {
			let samples = domain.collect(&self.collectors);
			if samples.is_empty() {
				continue;
			}

			let rows: Vec<Params> = samples
				.iter()
				.map(|s| {
					let mut map = HashMap::with_capacity(5);
					map.insert("ts".to_string(), Value::DateTime(ts));
					map.insert("scope".to_string(), Value::Utf8(s.scope.to_string()));
					map.insert("metric".to_string(), Value::Utf8(s.metric.to_string()));
					map.insert("value".to_string(), f64_value(s.value));
					map.insert("unit".to_string(), Value::Utf8(s.unit.to_string()));
					Params::Named(Arc::new(map))
				})
				.collect();

			let mut builder = self.engine.bulk_insert_unchecked(IdentityId::system());
			builder.series(domain.snapshots_path()).rows(rows).done();
			if let Err(e) = builder.execute() {
				error!("runtime metrics insert into {} failed: {e}", domain.snapshots_path());
				continue;
			}

			all.extend(samples);
		}

		if all.is_empty() {
			return;
		}

		let get = |name: &str| all.iter().find(|s| s.metric == name).map(|s| s.value).unwrap_or(0.0);
		info!(
			rss_anon_mb = get("rss_anon_bytes") / 1_048_576.0,
			rss_file_mb = get("rss_file_bytes") / 1_048_576.0,
			heap_live_mb = get("heap_live_bytes") / 1_048_576.0,
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
