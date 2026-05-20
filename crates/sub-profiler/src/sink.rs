// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::event::EventBus;
use reifydb_profiler::{
	event::{ProfilerScopeBatchEvent, ProfilerScopeClosedEvent},
	record::MinimalSpanRecord,
	sink::ProfilerSink,
	summary::ProfilerSummary,
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

impl ProfilerSink for EventBusSink {
	fn on_span_record(&self, record: &MinimalSpanRecord) {
		observe_record(record);
	}

	fn on_scope_closed(&self, summary: &ProfilerSummary) {
		self.event_bus.emit(ProfilerScopeClosedEvent::new(Arc::new(summary.clone())));
	}

	fn on_scope_batch(&self, summary: &ProfilerSummary) {
		self.event_bus.emit(ProfilerScopeBatchEvent::new(Arc::new(summary.clone())));
	}
}
