// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc, time::Duration as StdDuration};

use parking_lot::RwLock;
use reifydb_core::{
	event::{
		EventBus,
		metric::{ProfilerAggregateRow, ProfilerSnapshotEvent},
	},
	profiler::ProfilerCategoryId,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_profiler::{category::ProfilerCategory, record::AggregateRecord};
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};
use reifydb_type::{
	params::Params,
	value::{Value, datetime::DateTime, identity::IdentityId, r#type::Type},
};
use tracing::error;

use crate::{
	accumulator::ProfilerAccumulator,
	histograms::{
		PROFILER_SNAPSHOT_FLUSH_ERRORS, PROFILER_SNAPSHOT_LAST_FLUSH_RECORDS,
		PROFILER_SNAPSHOT_LAST_FLUSH_TS_MS,
	},
};

pub const DEFAULT_SNAPSHOT_INTERVAL: StdDuration = StdDuration::from_secs(10);

#[derive(Clone, Debug)]
pub enum SnapshotMessage {
	Tick(DateTime),
}

pub struct ProfilerSnapshotActor {
	accumulator: Arc<RwLock<ProfilerAccumulator>>,
	engine: StandardEngine,
	event_bus: EventBus,
	tick_interval: StdDuration,
}

impl ProfilerSnapshotActor {
	pub fn new(accumulator: Arc<RwLock<ProfilerAccumulator>>, engine: StandardEngine, event_bus: EventBus) -> Self {
		Self {
			accumulator,
			engine,
			event_bus,
			tick_interval: DEFAULT_SNAPSHOT_INTERVAL,
		}
	}

	pub fn with_interval(mut self, interval: StdDuration) -> Self {
		self.tick_interval = interval;
		self
	}

	pub fn flush(&self, ts: DateTime) {
		let snapshot = self.accumulator.read().snapshot();
		if snapshot.is_empty() {
			return;
		}

		let mut by_category: HashMap<ProfilerCategory, Vec<AggregateRecord>> = HashMap::new();
		for record in snapshot {
			by_category.entry(record.category).or_default().push(record);
		}

		let mut event_rows: Vec<ProfilerAggregateRow> = Vec::new();
		let mut total_written = 0usize;
		let mut had_error = false;

		for (category, records) in &by_category {
			let series_path = format!("system::metrics::profiler::{}::snapshots", category.name());
			let rows: Vec<Params> = records.iter().map(|r| record_to_params(r, ts)).collect();

			let mut builder = self.engine.bulk_insert_unchecked(IdentityId::system());
			builder.series(&series_path).rows(rows).done();
			if let Err(e) = builder.execute() {
				error!("profile snapshot insert into {series_path} failed: {e}");
				PROFILER_SNAPSHOT_FLUSH_ERRORS.inc();
				had_error = true;
			} else {
				total_written += records.len();
				for record in records {
					event_rows.push(aggregate_to_row(record));
				}
			}
		}

		if !had_error && !event_rows.is_empty() {
			PROFILER_SNAPSHOT_LAST_FLUSH_RECORDS.set(total_written as f64);
			PROFILER_SNAPSHOT_LAST_FLUSH_TS_MS.set(ts.to_nanos() as f64 / 1_000_000.0);
			self.event_bus.emit(ProfilerSnapshotEvent::new(ts, event_rows));
		}
	}
}

impl Actor for ProfilerSnapshotActor {
	type Message = SnapshotMessage;
	type State = ();

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_tick(self.tick_interval, |nanos| SnapshotMessage::Tick(DateTime::from_nanos(nanos)));
	}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			SnapshotMessage::Tick(ts) => self.flush(ts),
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}

fn aggregate_to_row(record: &AggregateRecord) -> ProfilerAggregateRow {
	let p = record.histogram.percentiles();
	let min_us = record.histogram.percentile(0.0);
	let max_us = record.histogram.percentile(1.0);
	ProfilerAggregateRow {
		category: ProfilerCategoryId(record.category as u8),
		span_name: record.span_name.clone(),
		dim_1: record.dimensions.first().cloned(),
		dim_2: record.dimensions.get(1).cloned(),
		calls: record.calls,
		total_us: record.total_us,
		min_us,
		max_us,
		p50_us: p.p50,
		p60_us: p.p60,
		p70_us: p.p70,
		p75_us: p.p75,
		p80_us: p.p80,
		p85_us: p.p85,
		p90_us: p.p90,
		p95_us: p.p95,
		p98_us: p.p98,
		p99_us: p.p99,
		extras_sum: *record.extras(),
	}
}

fn record_to_params(record: &AggregateRecord, ts: DateTime) -> Params {
	let mut map = HashMap::new();
	map.insert("ts".to_string(), Value::DateTime(ts));
	map.insert("span_name".to_string(), Value::Utf8(record.span_name.clone()));
	map.insert(
		"dim_1".to_string(),
		record.dimensions.first().map(|s| Value::Utf8(s.clone())).unwrap_or_else(|| Value::none_of(Type::Utf8)),
	);
	map.insert(
		"dim_2".to_string(),
		record.dimensions.get(1).map(|s| Value::Utf8(s.clone())).unwrap_or_else(|| Value::none_of(Type::Utf8)),
	);
	map.insert("calls".to_string(), Value::Uint8(record.calls));
	map.insert("total".to_string(), Value::Duration(record.total()));
	map.insert("min".to_string(), Value::Duration(record.min()));
	map.insert("max".to_string(), Value::Duration(record.max()));
	let p = record.percentiles();
	map.insert("p50".to_string(), Value::Duration(p.p50));
	map.insert("p60".to_string(), Value::Duration(p.p60));
	map.insert("p70".to_string(), Value::Duration(p.p70));
	map.insert("p75".to_string(), Value::Duration(p.p75));
	map.insert("p80".to_string(), Value::Duration(p.p80));
	map.insert("p85".to_string(), Value::Duration(p.p85));
	map.insert("p90".to_string(), Value::Duration(p.p90));
	map.insert("p95".to_string(), Value::Duration(p.p95));
	map.insert("p98".to_string(), Value::Duration(p.p98));
	map.insert("p99".to_string(), Value::Duration(p.p99));
	let extras = record.extras();
	map.insert("extra_0".to_string(), Value::Uint8(extras[0]));
	map.insert("extra_1".to_string(), Value::Uint8(extras[1]));
	map.insert("extra_2".to_string(), Value::Uint8(extras[2]));
	map.insert("extra_3".to_string(), Value::Uint8(extras[3]));
	Params::Named(Arc::new(map))
}
