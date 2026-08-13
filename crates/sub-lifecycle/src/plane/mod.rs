// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The retention plane: retention rules and floors above the lane that executes them.
//!
//! The lane answers when a class gets a slice; the plane answers up to which version it may reclaim ([`ledger`]),
//! how far back the epoch must stay answerable ([`horizon`]), and how far behind each class is ([`metrics`]).
//! One ledger keeps every floor inspectable instead of each executor computing its own cutoff privately.

pub mod horizon;
pub mod ledger;
pub mod measured;

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	interface::store::EntryKind,
	lifecycle::{
		class::{Floor, FloorTerm, RetentionClass},
		metrics::{ClassSnapshot, RetentionMetrics, StuckOnset},
		watermark::EvictionWatermark,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{context::clock::Clock, version_epoch::VersionEpoch};
use reifydb_value::value::{datetime::DateTime, duration::Duration};
use tracing::warn;

use crate::plane::ledger::{EngineFloors, FloorScope, FloorSource, HorizonLedger};

/// One per process: the floor source and accounting surface shared by every executor.
#[derive(Clone)]
pub struct RetentionPlane {
	inner: Arc<Inner>,
}

struct Inner {
	ledger: HorizonLedger,
	metrics: RetentionMetrics,
}

impl RetentionPlane {
	pub fn new(source: Arc<dyn FloorSource>, epoch: VersionEpoch) -> Self {
		Self::with_metrics(source, epoch, RetentionMetrics::new())
	}

	pub fn with_metrics(source: Arc<dyn FloorSource>, epoch: VersionEpoch, metrics: RetentionMetrics) -> Self {
		Self {
			inner: Arc::new(Inner {
				ledger: HorizonLedger::new(source, epoch),
				metrics,
			}),
		}
	}

	pub fn for_engine(engine: &StandardEngine, metrics: RetentionMetrics) -> Self {
		Self::with_metrics(Arc::new(EngineFloors::new(engine.clone())), engine.version_epoch().clone(), metrics)
	}

	pub fn cutoff(&self, class: RetentionClass, now: DateTime, ttl: Option<Duration>) -> Option<Floor> {
		self.inner.ledger.cutoff(class, FloorScope::Global, now, ttl)
	}

	pub fn cutoff_with_binding(
		&self,
		class: RetentionClass,
		now: DateTime,
		ttl: Option<Duration>,
	) -> Option<(Floor, FloorTerm)> {
		self.inner.ledger.cutoff_with_binding(class, FloorScope::Global, now, ttl)
	}

	pub fn kind_cutoff_with_binding(
		&self,
		class: RetentionClass,
		kind: EntryKind,
		now: DateTime,
		ttl: Option<Duration>,
	) -> Option<(Floor, FloorTerm)> {
		self.inner.ledger.cutoff_with_binding(class, FloorScope::Kind(kind), now, ttl)
	}

	pub fn expiry_cutoff(&self, now: DateTime, ttl: Duration) -> Option<CommitVersion> {
		self.inner.ledger.expiry_cutoff(now, ttl)
	}

	pub fn record_liveness(&self, class: RetentionClass) {
		self.inner.metrics.record_liveness(class);
	}

	pub fn record_reclamation(
		&self,
		class: RetentionClass,
		floor: Option<(Floor, FloorTerm)>,
		work_done: u64,
		backlog_hint: u64,
	) {
		match self.inner.metrics.record_reclamation(class, floor, work_done, backlog_hint) {
			StuckOnset::Quiet => {}
			StuckOnset::FloorPinned {
				floor,
				binding,
				backlog_hint,
			} => warn!(
				class = class.name(),
				floor = %floor,
				binding = %binding,
				protects = binding.protects(),
				backlog = backlog_hint,
				"retention class has eligible work but its floor will not advance"
			),
			StuckOnset::Starved {
				binding,
				backlog_hint,
			} => warn!(
				class = class.name(),
				binding = %binding,
				ages_by = binding.protects(),
				backlog = backlog_hint,
				"retention class has not reclaimed anything while its backlog persisted"
			),
			StuckOnset::FloorUnresolvable => warn!(
				class = class.name(),
				"retention class has no resolvable floor; it can reclaim nothing"
			),
		}
	}

	pub fn record_budget_exhausted(&self, class: RetentionClass) {
		self.inner.metrics.record_budget_exhausted(class);
	}

	pub fn record_gated(&self, class: RetentionClass) {
		self.inner.metrics.record_gated(class);
	}

	pub fn snapshot(&self, class: RetentionClass) -> ClassSnapshot {
		self.inner.metrics.snapshot(class)
	}

	pub fn report(&self) -> Vec<(RetentionClass, ClassSnapshot)> {
		self.inner.metrics.report()
	}

	/// The flush tier's cutoff, resolved from the [`RetentionClass::PersistentFlush`] floor terms.
	pub fn eviction_watermark(&self, clock: Clock) -> Arc<dyn EvictionWatermark> {
		Arc::new(PlaneEvictionWatermark {
			plane: self.clone(),
			clock,
		})
	}
}

struct PlaneEvictionWatermark {
	plane: RetentionPlane,
	clock: Clock,
}

impl EvictionWatermark for PlaneEvictionWatermark {
	fn watermark(&self) -> CommitVersion {
		let now = self.clock.now();
		match self.plane.cutoff(RetentionClass::PersistentFlush, now, None) {
			Some(Floor::Version(version)) => version,
			Some(Floor::Instant(_)) | None => CommitVersion(0),
		}
	}
}
