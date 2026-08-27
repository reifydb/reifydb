// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{lifecycle::progress::Progress, util::budget::MemoryBudget};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, value::duration::Duration};

use crate::tier::commit::{
	CommitConfig, CommitDomain, CommitKindMetrics, CommitMetrics, CommitTier, CommitWaker, FlushOutcome, Inner,
	Settlement, Shared, Slice,
};

impl<D: CommitDomain> CommitTier<D> {
	pub fn new(config: CommitConfig, build: impl FnOnce(Arc<MemoryBudget>) -> D::State) -> Option<Self> {
		let limit = config.budget?;
		let budget = Arc::new(MemoryBudget::new(limit));
		let state = build(budget.clone());
		Some(Self {
			inner: Arc::new(Shared {
				state,
				inner: Mutex::new(Inner {
					triggered: false,
					resume_from: None,
					metrics: CommitMetrics::default(),
					kinds: Vec::new(),
				}),
				flush: Mutex::new(()),
				budget,
				interval: config.interval,
				waker: Mutex::new(None),
			}),
		})
	}

	pub fn state(&self) -> &D::State {
		&self.inner.state
	}

	pub fn budget(&self) -> &MemoryBudget {
		&self.inner.budget
	}

	pub fn interval(&self) -> Duration {
		self.inner.interval
	}

	pub fn resident_bytes(&self) -> ByteSize {
		D::resident_bytes(&self.inner.state)
	}

	pub fn attach_waker(&self, waker: Arc<dyn CommitWaker>) {
		*self.inner.waker.lock() = Some(waker);
	}

	pub fn is_triggered(&self) -> bool {
		self.inner.inner.lock().triggered
	}

	pub fn admits_write(&self) -> bool {
		D::admits_over_budget_writes() || !self.inner.budget.over_budget()
	}

	pub fn observe_write(&self) {
		if !self.inner.budget.over_budget() {
			return;
		}
		{
			let mut inner = self.inner.inner.lock();
			if inner.triggered {
				return;
			}
			inner.triggered = true;
			inner.metrics.wakes += 1;
		}
		let waker = self.inner.waker.lock().clone();
		if let Some(waker) = waker {
			waker.wake();
		}
	}

	pub fn flush_slice(&self, budget: ByteSize) -> FlushOutcome {
		let Some(cutoff) = D::cutoff(&self.inner.state) else {
			return FlushOutcome::exhausted();
		};
		let mut outcome = self.run_slice(cutoff, budget, false);
		outcome.backlog = self.resident_bytes();
		outcome
	}

	pub fn flush_pending(&self) -> FlushOutcome {
		self.paginate(None, D::MAX_SLICES_PER_TICK)
	}

	pub fn flush_all(&self) -> FlushOutcome {
		self.paginate(Some(D::cutoff_all()), usize::MAX)
	}

	pub fn metrics(&self) -> CommitMetrics {
		let mut metrics = self.inner.inner.lock().metrics;
		metrics.backlog = self.resident_bytes();
		metrics
	}

	pub fn kind_metrics(&self) -> Vec<CommitKindMetrics<D>> {
		self.inner
			.inner
			.lock()
			.kinds
			.iter()
			.map(|(kind, counters)| CommitKindMetrics {
				kind: *kind,
				counters: *counters,
			})
			.collect()
	}

	fn paginate(&self, drain: Option<D::Cutoff>, slices: usize) -> FlushOutcome {
		let budget = self.inner.budget.limit();
		let mut outcome = FlushOutcome::exhausted();
		for _ in 0..slices {
			let Some(cutoff) = drain.or_else(|| D::cutoff(&self.inner.state)) else {
				break;
			};
			let slice = self.run_slice(cutoff, budget, drain.is_some());
			outcome.slices += slice.slices;
			outcome.persisted += slice.persisted;
			outcome.reclaimed += slice.reclaimed;
			outcome.released = outcome.released.saturating_add(slice.released);
			outcome.progress = slice.progress;
			if slice.progress.is_exhausted() || slice.slices == 0 {
				break;
			}
		}
		outcome.backlog = self.resident_bytes();
		outcome
	}

	fn run_slice(&self, cutoff: D::Cutoff, budget: ByteSize, drain: bool) -> FlushOutcome {
		let _guard = self.inner.flush.lock();

		if !drain && !D::worth_persisting(D::resident_bytes(&self.inner.state)) {
			return FlushOutcome::exhausted();
		}

		let mut kinds = D::kinds(&self.inner.state);
		if kinds.is_empty() {
			return FlushOutcome::exhausted();
		}
		if let Some(resume) = self.inner.inner.lock().resume_from
			&& let Some(position) = kinds.iter().position(|kind| *kind == resume)
		{
			kinds.rotate_left(position);
		}

		let mut outcome = FlushOutcome::exhausted();
		let mut remaining = budget;
		let mut more = false;
		let mut stopped_at = None;

		for kind in kinds {
			if remaining == ByteSize::ZERO {
				more = true;
				stopped_at = Some(kind);
				self.inner.inner.lock().metrics.budget_exhausted += 1;
				break;
			}

			let Some(slice) = self.take(kind, cutoff, remaining) else {
				continue;
			};
			remaining = remaining.saturating_sub(slice.bytes);
			more |= slice.more;

			let ack = D::persist(&self.inner.state, &slice.batch).expect("commit tier persist failed");
			let settlement = self.settle(slice.batch, ack);

			outcome.slices += 1;
			outcome.persisted += settlement.entries;
			outcome.reclaimed += settlement.reclaimed;
			outcome.released = outcome.released.saturating_add(settlement.released);
			self.record(kind, settlement);
		}

		self.inner.inner.lock().resume_from = stopped_at;
		outcome.progress = if more {
			Progress::Yielded
		} else {
			Progress::Exhausted
		};
		outcome
	}

	pub fn take(&self, kind: D::Kind, cutoff: D::Cutoff, budget: ByteSize) -> Option<Slice<D>> {
		let _inner = self.inner.inner.lock();
		let slice = D::select(&self.inner.state, kind, cutoff, budget)?;
		Some(slice)
	}

	pub fn settle(&self, batch: D::Batch, ack: D::Ack) -> Settlement {
		let settlement = {
			let mut inner = self.inner.inner.lock();
			let settlement = D::settle(&self.inner.state, batch, ack);
			inner.triggered = false;
			settlement
		};
		self.inner.budget.release(settlement.released);
		reifydb_assertions! {
			let census = D::census(&self.inner.state);
			assert_eq!(
				census.counted, census.walked,
				"{} commit tier byte counter drifted: the budget carries {}, the resident set walks to {}",
				D::SCOPE, census.counted, census.walked
			);
		}
		settlement
	}

	fn record(&self, kind: D::Kind, settlement: Settlement) {
		let mut inner = self.inner.inner.lock();
		inner.metrics.slices += 1;
		inner.metrics.persisted += settlement.entries;
		inner.metrics.released = inner.metrics.released.saturating_add(settlement.released);

		let position = match inner.kinds.iter().position(|(candidate, _)| *candidate == kind) {
			Some(position) => position,
			None => {
				inner.kinds.push((kind, CommitMetrics::default()));
				inner.kinds.len() - 1
			}
		};
		let counters = &mut inner.kinds[position].1;
		counters.slices += 1;
		counters.persisted += settlement.entries;
		counters.released = counters.released.saturating_add(settlement.released);
	}
}
