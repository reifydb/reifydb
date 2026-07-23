// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The retention plane: retention rules and floors above the lane that executes them.
//!
//! The lane in `crate::actor` answers "when does this class get a slice". The plane answers the three questions it
//! cannot: up to which version may this class reclaim ([`ledger`]), how far back must the epoch stay answerable
//! ([`horizon`]), and how far behind is each class right now ([`metrics`]).
//!
//! Splitting floors out of the executors is the point. Eight classes each computing their own cutoff privately is how
//! a wrong or wedged floor stayed invisible until disk grew; a single ledger makes every floor inspectable, and
//! makes it structural that a class is constrained by exactly the readers its class declares.

pub mod horizon;
pub mod ledger;
pub mod measured;
pub mod metrics;

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	lifecycle::{
		class::{FloorTerm, RetentionClass},
		watermark::EvictionWatermark,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{context::clock::Clock, version_epoch::VersionEpoch};
use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::plane::{
	ledger::{EngineFloors, FloorSource, HorizonLedger},
	metrics::{ClassSnapshot, RetentionMetrics},
};

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
		Self {
			inner: Arc::new(Inner {
				ledger: HorizonLedger::new(source, epoch),
				metrics: RetentionMetrics::new(),
			}),
		}
	}

	pub fn for_engine(engine: &StandardEngine) -> Self {
		Self::new(Arc::new(EngineFloors::new(engine.clone())), engine.version_epoch().clone())
	}

	pub fn cutoff(&self, class: RetentionClass, now: DateTime, ttl: Option<Duration>) -> Option<CommitVersion> {
		self.inner.ledger.cutoff(class, now, ttl)
	}

	pub fn cutoff_with_binding(
		&self,
		class: RetentionClass,
		now: DateTime,
		ttl: Option<Duration>,
	) -> Option<(CommitVersion, FloorTerm)> {
		self.inner.ledger.cutoff_with_binding(class, now, ttl)
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
		floor: Option<(CommitVersion, FloorTerm)>,
		work_done: u64,
		backlog_hint: u64,
	) {
		self.inner.metrics.record_reclamation(class, floor, work_done, backlog_hint);
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
		self.plane.cutoff(RetentionClass::PersistentFlush, now, None).unwrap_or(CommitVersion(0))
	}
}
