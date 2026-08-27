// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod domain;
mod driver;
mod metrics;
#[cfg(test)]
mod tests;

use std::{borrow::Cow, fmt::Debug, sync::Arc};

pub use metrics::{CommitKindMetrics, CommitMetrics};
use reifydb_core::{lifecycle::progress::Progress, util::budget::MemoryBudget};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

pub trait CommitDomain: Copy + Debug + 'static {
	type State: Send + Sync + 'static;
	type Batch: Send + Sync + 'static;
	type Ack: Send + Sync + 'static;
	type Cutoff: Copy + Debug + Send + Sync + 'static;
	type Kind: Copy + Eq + Debug + Send + Sync + 'static;

	const SCOPE: &'static str;

	const MAX_SLICES_PER_TICK: usize;

	fn cutoff(state: &Self::State) -> Option<Self::Cutoff>;

	fn cutoff_all() -> Self::Cutoff;

	fn kinds(state: &Self::State) -> Vec<Self::Kind>;

	fn select(state: &Self::State, kind: Self::Kind, cutoff: Self::Cutoff, budget: ByteSize)
	-> Option<Slice<Self>>;

	fn persist(state: &Self::State, batch: &Self::Batch) -> reifydb_value::Result<Self::Ack>;

	fn settle(state: &Self::State, batch: Self::Batch, ack: Self::Ack) -> Settlement;

	fn resident_bytes(state: &Self::State) -> ByteSize;

	fn kind_name(kind: Self::Kind) -> Cow<'static, str>;

	fn census(state: &Self::State) -> CommitCensus;

	fn worth_persisting(bytes: ByteSize) -> bool {
		let _ = bytes;
		true
	}
	fn admits_over_budget_writes() -> bool {
		true
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitCensus {
	pub counted: ByteSize,
	pub walked: ByteSize,
}

pub struct Slice<D: CommitDomain> {
	pub batch: D::Batch,
	pub bytes: ByteSize,
	pub more: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Settlement {
	pub released: ByteSize,
	pub entries: u64,
	pub reclaimed: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushOutcome {
	pub progress: Progress,
	pub slices: u64,
	pub persisted: u64,
	pub reclaimed: u64,
	pub released: ByteSize,
	pub backlog: ByteSize,
}

impl FlushOutcome {
	pub fn exhausted() -> Self {
		Self {
			progress: Progress::Exhausted,
			slices: 0,
			persisted: 0,
			reclaimed: 0,
			released: ByteSize::ZERO,
			backlog: ByteSize::ZERO,
		}
	}

	pub fn is_exhausted(&self) -> bool {
		self.progress.is_exhausted()
	}

	pub fn is_yielded(&self) -> bool {
		self.progress.is_yielded()
	}
}

pub trait CommitWaker: Send + Sync + 'static {
	fn wake(&self);
}

#[derive(Clone, Copy, Debug)]
pub struct CommitConfig {
	pub budget: Option<ByteSize>,
	pub interval: Duration,
}

impl CommitConfig {
	pub fn testing() -> Self {
		Self {
			budget: Some(ByteSize::from_mib(4)),
			interval: Duration::from_seconds_const(5),
		}
	}
}

struct Inner<D: CommitDomain> {
	triggered: bool,
	resume_from: Option<D::Kind>,
	metrics: CommitMetrics,
	kinds: Vec<(D::Kind, CommitMetrics)>,
}

struct Shared<D: CommitDomain> {
	state: D::State,
	inner: Mutex<Inner<D>>,
	flush: Mutex<()>,
	budget: Arc<MemoryBudget>,
	interval: Duration,
	waker: Mutex<Option<Arc<dyn CommitWaker>>>,
}

pub struct CommitTier<D: CommitDomain> {
	inner: Arc<Shared<D>>,
}

impl<D: CommitDomain> Clone for CommitTier<D> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}
