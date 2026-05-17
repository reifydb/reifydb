// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! `ProfileSink` implementation that bridges the profiler primitives to the reifydb event bus and the static
//! per-category histograms. Per-span observations go straight to the lock-free histogram on the hot path; per-scope
//! summaries are emitted as `ProfileScopeClosedEvent`/`ProfileScopeBatchEvent` so the listener can forward them to
//! the collector actor.

use std::sync::Arc;

use reifydb_core::event::EventBus;
use reifydb_profiler::{
	event::{ProfileScopeBatchEvent, ProfileScopeClosedEvent},
	record::MinimalSpanRecord,
	sink::ProfileSink,
	summary::ProfileSummary,
};

use crate::actor::observe_record;

pub struct EventBusSink {
	event_bus: EventBus,
}

impl EventBusSink {
	pub fn new(event_bus: EventBus) -> Self {
		Self {
			event_bus,
		}
	}
}

impl ProfileSink for EventBusSink {
	fn on_span_record(&self, record: &MinimalSpanRecord) {
		observe_record(record);
	}

	fn on_scope_closed(&self, summary: &ProfileSummary) {
		self.event_bus.emit(ProfileScopeClosedEvent::new(Arc::new(summary.clone())));
	}

	fn on_scope_batch(&self, summary: &ProfileSummary) {
		self.event_bus.emit(ProfileScopeBatchEvent::new(Arc::new(summary.clone())));
	}
}
