// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::{collect::MetricsCollector, sample::MetricsSample};
use reifydb_value::byte_size::ByteSize;

use crate::tier::commit::{CommitDomain, CommitTier};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitMetrics {
	/// Batches taken from a kind and settled.
	pub slices: u64,
	/// Entries the persistent tier acknowledged.
	pub persisted: u64,
	pub released: ByteSize,
	/// Bytes still resident, live plus in flight; a gauge, read at sample time rather than accumulated.
	pub backlog: ByteSize,
	/// Slices that stopped because the byte budget ran out rather than because the cutoff admitted nothing.
	pub budget_exhausted: u64,
	/// Full-buffer wakes raised; one per crossing of the budget, not one per write over it.
	pub wakes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct CommitKindMetrics<D: CommitDomain> {
	pub kind: D::Kind,
	pub counters: CommitMetrics,
}

impl<D: CommitDomain> MetricsCollector for CommitTier<D> {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let counters = self.metrics();
		out.push(MetricsSample::heap(D::SCOPE, "resident_bytes", counters.backlog));
		out.push(MetricsSample::bytes(D::SCOPE, "budget_bytes", self.budget().limit()));
		out.push(MetricsSample::counter(D::SCOPE, "slices", counters.slices));
		out.push(MetricsSample::counter(D::SCOPE, "persisted", counters.persisted));
		out.push(MetricsSample::bytes(D::SCOPE, "released_bytes", counters.released));
		out.push(MetricsSample::counter(D::SCOPE, "budget_exhausted", counters.budget_exhausted));
		out.push(MetricsSample::counter(D::SCOPE, "wakes", counters.wakes));
		for kind in self.kind_metrics() {
			let scope = format!("{}::kind::{}", D::SCOPE, D::kind_name(kind.kind));
			out.push(MetricsSample::counter(scope.clone(), "slices", kind.counters.slices));
			out.push(MetricsSample::counter(scope.clone(), "persisted", kind.counters.persisted));
			out.push(MetricsSample::bytes(scope, "released_bytes", kind.counters.released));
		}
	}
}
