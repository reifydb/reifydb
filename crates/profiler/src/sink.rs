// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::{record::MinimalSpanRecord, summary::ProfilerSummary};

pub trait ProfilerSink: Send + Sync + 'static {
	fn on_span_record(&self, _record: &MinimalSpanRecord) {}

	fn on_scope_closed(&self, summary: &ProfilerSummary);

	fn on_scope_batch(&self, summary: &ProfilerSummary);
}

pub struct NoopSink;

impl ProfilerSink for NoopSink {
	fn on_scope_closed(&self, _summary: &ProfilerSummary) {}

	fn on_scope_batch(&self, _summary: &ProfilerSummary) {}
}

pub fn noop_sink() -> Arc<dyn ProfilerSink> {
	Arc::new(NoopSink)
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::*;
	use crate::{
		category::CATEGORY_COUNT,
		record::MAX_EXTRAS,
		scope::ScopeId,
		summary::{CategorySummary, ProfilerSummary},
	};

	#[derive(Default)]
	struct CountingSink {
		closed: AtomicUsize,
		batched: AtomicUsize,
	}

	impl ProfilerSink for CountingSink {
		fn on_scope_closed(&self, _summary: &ProfilerSummary) {
			self.closed.fetch_add(1, Ordering::Relaxed);
		}

		fn on_scope_batch(&self, _summary: &ProfilerSummary) {
			self.batched.fetch_add(1, Ordering::Relaxed);
		}
	}

	#[test]
	fn noop_sink_compiles_and_runs() {
		let sink = noop_sink();
		let summary = ProfilerSummary {
			scope_id: ScopeId(0),
			scope_name: "test",
			started_at_nanos: 0,
			total_duration_us: 0,
			records: Vec::new(),
			per_category: [CategorySummary::default(); CATEGORY_COUNT],
			interner: None,
		};
		sink.on_scope_closed(&summary);
		sink.on_scope_batch(&summary);
	}

	#[test]
	fn counting_sink_observes_calls() {
		let sink = CountingSink::default();
		let summary = ProfilerSummary {
			scope_id: ScopeId(0),
			scope_name: "test",
			started_at_nanos: 0,
			total_duration_us: 0,
			records: Vec::new(),
			per_category: [CategorySummary::default(); CATEGORY_COUNT],
			interner: None,
		};
		sink.on_scope_closed(&summary);
		sink.on_scope_closed(&summary);
		sink.on_scope_batch(&summary);
		assert_eq!(sink.closed.load(Ordering::Relaxed), 2);
		assert_eq!(sink.batched.load(Ordering::Relaxed), 1);
	}

	const _: [u8; MAX_EXTRAS] = [0; 4];
}
