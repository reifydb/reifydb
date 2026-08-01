// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

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
use reifydb_value::value::duration::Duration;

use super::{accumulator::ProfilerAccumulator, instruments::ProfilerInstruments, publish::spans_columns};
use crate::framework::current::CurrentCache;

#[derive(Clone)]
pub enum ProfilerMessage {
	ScopeClosed(Arc<ProfilerSummary>),
	ScopeBatch(Arc<ProfilerSummary>),
	Wire {
		current_cache: CurrentCache,
		total_cache: CurrentCache,
		interval: Duration,
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
}

pub struct ProfilerActorState {
	processed_summaries: u64,
	processed_batches: u64,
	processed_records: u64,
	horizon: ProfilerAccumulator,
	targets: Option<PublishTargets>,
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
			acc.upsert(ident, span_name, record.duration_us, &record.extras, &self.interner);
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
		targets.current_cache.store(spans_columns(&mut window_records, now));
		for (ident, record) in drained {
			state.horizon.absorb(ident, record);
		}
		let mut horizon_records = state.horizon.all();
		targets.total_cache.store(spans_columns(&mut horizon_records, now));
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
		Ffi => "ffi",
		Cache => "cache",
		RowShape => "row_shape",
		Api => "api",
		Actor => "actor",
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
			} => {
				let schedule = state.targets.is_none();
				state.targets = Some(PublishTargets {
					current_cache,
					total_cache,
					interval,
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
