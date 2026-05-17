// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Single-writer actor that owns the deep-dive store and processes profile events off the worker threads. The layer
//! observes histograms and pushes records into the shared scope state on the hot path; the sink emits one event per
//! scope close. This actor consumes those events and folds records into the `ProfileAccumulator` (a transient
//! in-memory buffer; long-term storage is the metric subsystem's responsibility).

use std::sync::Arc;

use parking_lot::RwLock;
use reifydb_profiler::{
	callsite,
	category::{ProfileCategory, ProfileCategory::*},
	intern::DimInterner,
	record::{MAX_EXTRAS, MinimalSpanRecord, SpanIdent},
	summary::ProfileSummary,
};
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};

use crate::{accumulator::ProfileAccumulator, histograms::histogram_for};

#[derive(Clone, Debug)]
pub enum ProfilerMessage {
	ScopeClosed(Arc<ProfileSummary>),
	ScopeBatch(Arc<ProfileSummary>),
}

pub struct ProfileCollectorActor {
	accumulator: Arc<RwLock<ProfileAccumulator>>,
	interner: Arc<DimInterner>,
}

pub struct ProfileActorState {
	processed_summaries: u64,
	processed_batches: u64,
	processed_records: u64,
}

impl ProfileCollectorActor {
	pub fn new(accumulator: Arc<RwLock<ProfileAccumulator>>, interner: Arc<DimInterner>) -> Self {
		Self {
			accumulator,
			interner,
		}
	}

	fn apply_summary(&self, summary: &ProfileSummary, state: &mut ProfileActorState) {
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
}

fn span_name_for(category: ProfileCategory) -> &'static str {
	match category {
		Query => "query",
		Txn => "txn",
		Storage => "storage",
		Plan => "plan",
		Cdc => "cdc",
		Flow => "flow",
	}
}

impl Actor for ProfileCollectorActor {
	type Message = ProfilerMessage;
	type State = ProfileActorState;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		ProfileActorState {
			processed_summaries: 0,
			processed_batches: 0,
			processed_records: 0,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			ProfilerMessage::ScopeClosed(summary) => {
				self.apply_summary(&summary, state);
				state.processed_summaries = state.processed_summaries.saturating_add(1);
			}
			ProfilerMessage::ScopeBatch(summary) => {
				self.apply_summary(&summary, state);
				state.processed_batches = state.processed_batches.saturating_add(1);
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}

pub fn observe_record(record: &MinimalSpanRecord) {
	histogram_for(record.category()).observe(record.duration_us as f64);
}

#[allow(dead_code)]
const _ASSERT_EXTRAS: usize = MAX_EXTRAS;
