// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::{record::MinimalSpanRecord, summary::ProfileSummary};

pub trait ProfileSink: Send + Sync + 'static {
	fn on_span_record(&self, _record: &MinimalSpanRecord) {}

	fn on_scope_closed(&self, summary: &ProfileSummary);

	fn on_scope_batch(&self, summary: &ProfileSummary);
}

pub struct NoopSink;

impl ProfileSink for NoopSink {
	fn on_scope_closed(&self, _summary: &ProfileSummary) {}

	fn on_scope_batch(&self, _summary: &ProfileSummary) {}
}

pub fn noop_sink() -> Arc<dyn ProfileSink> {
	Arc::new(NoopSink)
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::*;
	use crate::{
		record::MAX_EXTRAS,
		scope::ScopeId,
		summary::{CategorySummary, ProfileSummary},
	};

	#[derive(Default)]
	struct CountingSink {
		closed: AtomicUsize,
		batched: AtomicUsize,
	}

	impl ProfileSink for CountingSink {
		fn on_scope_closed(&self, _summary: &ProfileSummary) {
			self.closed.fetch_add(1, Ordering::Relaxed);
		}

		fn on_scope_batch(&self, _summary: &ProfileSummary) {
			self.batched.fetch_add(1, Ordering::Relaxed);
		}
	}

	#[test]
	fn noop_sink_compiles_and_runs() {
		let sink = noop_sink();
		let summary = ProfileSummary {
			scope_id: ScopeId(0),
			scope_name: "test",
			started_at_nanos: 0,
			total_duration_us: 0,
			records: Vec::new(),
			per_category: [CategorySummary::default(); 6],
			interner: None,
		};
		sink.on_scope_closed(&summary);
		sink.on_scope_batch(&summary);
	}

	#[test]
	fn counting_sink_observes_calls() {
		let sink = CountingSink::default();
		let summary = ProfileSummary {
			scope_id: ScopeId(0),
			scope_name: "test",
			started_at_nanos: 0,
			total_duration_us: 0,
			records: Vec::new(),
			per_category: [CategorySummary::default(); 6],
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
