// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::value::column::columns::Columns;
use reifydb_engine::engine::StandardEngine;
use reifydb_profiler::{
	callsite,
	category::{ProfilerCategory, ProfilerCategory::*},
	intern::DimInterner,
	record::{MAX_EXTRAS, MinimalSpanRecord, SpanIdent},
	summary::ProfilerSummary,
};
use reifydb_runtime::{
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
	sync::rwlock::RwLock,
};
use reifydb_value::{
	params::Params,
	value::{Value, datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::error;

use super::{accumulator::ProfilerAccumulator, instruments::ProfilerInstruments, publish::spans_columns};
use crate::framework::{current::CurrentCache, spec::MetricsDomain};

#[derive(Clone)]
pub enum ProfilerMessage {
	ScopeClosed(Arc<ProfilerSummary>),
	ScopeBatch(Arc<ProfilerSummary>),
	Wire {
		current_cache: CurrentCache,
		total_cache: CurrentCache,
		interval: Duration,
		snapshot_interval: Option<Duration>,
		engine: StandardEngine,
	},
	Tick,
}

pub struct ProfilerCollectorActor {
	accumulator: Arc<RwLock<ProfilerAccumulator>>,
	interner: Arc<DimInterner>,
	instruments: Arc<ProfilerInstruments>,
	horizon_capacity: usize,
	horizon_retention_floor: u64,
	clock: Clock,
}

struct PublishTargets {
	current_cache: CurrentCache,
	total_cache: CurrentCache,
	interval: Duration,
	snapshot_interval: Option<Duration>,
	engine: StandardEngine,
}

pub struct ProfilerActorState {
	processed_summaries: u64,
	processed_batches: u64,
	processed_records: u64,
	horizon: ProfilerAccumulator,
	targets: Option<PublishTargets>,
	last_snapshot: Option<DateTime>,
}

impl ProfilerCollectorActor {
	pub fn new(
		accumulator: Arc<RwLock<ProfilerAccumulator>>,
		interner: Arc<DimInterner>,
		instruments: Arc<ProfilerInstruments>,
		horizon_capacity: usize,
		horizon_retention_floor: u64,
		clock: Clock,
	) -> Self {
		Self {
			accumulator,
			interner,
			instruments,
			horizon_capacity,
			horizon_retention_floor,
			clock,
		}
	}

	fn apply_summary(&self, summary: &ProfilerSummary, state: &mut ProfilerActorState) {
		let mut acc = self.accumulator.write();
		for record in &summary.records {
			let category = record.category();
			let ident = SpanIdent::new(category, record.callsite_id, record.dim_indices);
			let span_name =
				callsite::resolve(record.callsite_id).unwrap_or_else(|| span_name_for(category));
			acc.upsert(
				ident,
				span_name,
				record.duration_us,
				record.self_us,
				&record.extras,
				&self.interner,
			);
			state.processed_records = state.processed_records.saturating_add(1);
		}
	}

	fn publish(&self, state: &mut ProfilerActorState) {
		let Some(targets) = &state.targets else {
			return;
		};
		let drained = self.accumulator.write().drain();
		let mut window_records: Vec<_> = drained.iter().map(|(_, record)| record.clone()).collect();
		let now = self.clock.now();
		let current_columns = spans_columns(&mut window_records, now);
		if self.snapshot_due(targets, state.last_snapshot, now) {
			append_spans_snapshot(&targets.engine, &current_columns);
			state.last_snapshot = Some(now);
		}
		targets.current_cache.store(current_columns);
		for (ident, record) in drained {
			state.horizon.absorb(ident, record);
		}
		let mut horizon_records = state.horizon.all();
		targets.total_cache.store(spans_columns(&mut horizon_records, now));
	}

	fn snapshot_due(&self, targets: &PublishTargets, last: Option<DateTime>, now: DateTime) -> bool {
		let Some(interval) = targets.snapshot_interval else {
			return false;
		};
		match last {
			None => true,
			Some(last) => {
				now.to_nanos().saturating_sub(last.to_nanos()) >= interval.to_std().as_nanos() as u64
			}
		}
	}
}

fn append_spans_snapshot(engine: &StandardEngine, columns: &Columns) {
	let row_count = columns.get(0).map(|column| column.data().len()).unwrap_or(0);
	if row_count == 0 {
		return;
	}
	let rows: Vec<Params> = (0..row_count)
		.map(|index| {
			let mut row = HashMap::new();
			for column in columns.iter() {
				let value = column.data().get_value(index);
				if !matches!(value, Value::None { .. }) {
					row.insert(column.name().text().to_string(), value);
				}
			}
			Params::Named(Arc::new(row))
		})
		.collect();
	let mut builder = engine.bulk_insert_unchecked(IdentityId::system());
	builder.series(MetricsDomain::ProfilerSpans.snapshots_path()).rows(rows).done();
	if let Err(e) = builder.execute() {
		error!("Failed to append profiler spans snapshot: {}", e);
	}
}

fn span_name_for(category: ProfilerCategory) -> &'static str {
	match category {
		Query => "query",
		Txn => "txn",
		Storage => "storage",
		Plan => "plan",
		Cdc => "cdc",
		Flow => "flow",
		Subscription => "subscription",
		Server => "server",
		Wire => "wire",
		Auth => "auth",
		Catalog => "catalog",
		Engine => "engine",
		Mutate => "mutate",
		Transport => "transport",
		Task => "task",
		Policy => "policy",
		ExternC => "extern_c",
		Cache => "cache",
		RowShape => "row_shape",
		Api => "api",
		Actor => "actor",
		Lifecycle => "lifecycle",
	}
}

impl Actor for ProfilerCollectorActor {
	type Message = ProfilerMessage;
	type State = ProfilerActorState;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		ProfilerActorState {
			processed_summaries: 0,
			processed_batches: 0,
			processed_records: 0,
			horizon: ProfilerAccumulator::new(
				self.horizon_capacity,
				self.horizon_retention_floor,
				Arc::clone(&self.instruments),
			),
			targets: None,
			last_snapshot: None,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			ProfilerMessage::ScopeClosed(summary) => {
				self.apply_summary(&summary, state);
				state.processed_summaries = state.processed_summaries.saturating_add(1);
			}
			ProfilerMessage::ScopeBatch(summary) => {
				self.apply_summary(&summary, state);
				state.processed_batches = state.processed_batches.saturating_add(1);
			}
			ProfilerMessage::Wire {
				current_cache,
				total_cache,
				interval,
				snapshot_interval,
				engine,
			} => {
				let schedule = state.targets.is_none();
				state.targets = Some(PublishTargets {
					current_cache,
					total_cache,
					interval,
					snapshot_interval,
					engine,
				});
				if schedule {
					ctx.schedule_once(interval, || ProfilerMessage::Tick);
				}
			}
			ProfilerMessage::Tick => {
				self.publish(state);
				if let Some(targets) = &state.targets {
					ctx.schedule_once(targets.interval, || ProfilerMessage::Tick);
				}
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}

pub fn observe_record(instruments: &ProfilerInstruments, record: &MinimalSpanRecord) {
	instruments.histogram_for(record.category()).observe(record.duration_us as f64);
}

#[allow(dead_code)]
const _ASSERT_EXTRAS: usize = MAX_EXTRAS;
