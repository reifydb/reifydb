// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::metrics::sample::MetricsSample;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_value::{
	params::Params,
	value::{Value, datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::error;

use crate::domains::runtime::{Domain, SampleReader};

#[derive(Clone, Debug)]
pub enum MetricsSamplerMessage {
	Tick,
}

pub struct MetricsSamplerActor {
	engine: StandardEngine,
	reader: SampleReader,
	clock: Clock,
	interval: Duration,
}

impl MetricsSamplerActor {
	pub fn new(engine: StandardEngine, reader: SampleReader, clock: Clock, interval: Duration) -> Self {
		Self {
			engine,
			reader,
			clock,
			interval,
		}
	}

	fn sample(&self) {
		let now = DateTime::from_nanos(self.clock.now_nanos());
		for domain in Domain::ALL {
			let samples = self.reader.samples_for(domain);
			if samples.is_empty() {
				continue;
			}
			let rows: Vec<Params> = samples.iter().map(|sample| snapshot_row(now, sample)).collect();
			let series = format!("system::metrics::runtime::{}::snapshots", domain.local_name());
			let mut builder = self.engine.bulk_insert_unchecked(IdentityId::system());
			builder.series(&series).rows(rows).done();
			if let Err(e) = builder.execute() {
				error!("Failed to append {} metrics snapshots: {}", domain.local_name(), e);
			}
		}
	}
}

fn snapshot_row(now: DateTime, sample: &MetricsSample) -> Params {
	let mut row = HashMap::new();
	row.insert("ts".to_string(), Value::DateTime(now));
	row.insert("scope".to_string(), Value::Utf8(sample.scope.to_string()));
	row.insert("metric".to_string(), Value::Utf8(sample.metric.to_string()));
	row.insert("value".to_string(), Value::float8(sample.reading.as_f64()));
	row.insert("unit".to_string(), Value::Utf8(sample.reading.unit().to_string()));
	Params::Named(Arc::new(row))
}

impl Actor for MetricsSamplerActor {
	type Message = MetricsSamplerMessage;
	type State = ();

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.interval, || MetricsSamplerMessage::Tick);
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			MetricsSamplerMessage::Tick => {
				self.sample();
				ctx.schedule_once(self.interval, || MetricsSamplerMessage::Tick);
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}
