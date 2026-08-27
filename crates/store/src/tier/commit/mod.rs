// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Commit tier: the RAM-resident write set a store accumulates between flushes, and the driver that
//! moves a byte-bounded slice of it into the persistent tier. Holding writes here collapses repeated
//! writes to one key into a single device write; the byte budget is the width of that collapse window,
//! and the only thing standing between the window and unbounded RAM.
//!
//! What a slice is, how it is selected, how it is persisted and what settles after are the domain's to
//! answer, through [`CommitDomain`]; the tier never decodes a key, opens a transaction, or knows what a
//! version is. The tier owns only the control plane: the budget, the flush guard, the in-flight flag,
//! the idle condvar and the pagination cursor.
//!
//! Bytes are released when a batch settles, never when it is selected. A batch already taken out of the
//! resident set still occupies RAM until the persistent tier acknowledges it, so releasing at selection
//! would re-arm the full trigger while the previous batch is still in flight.

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

/// How one store's write set is sliced, persisted and settled.
///
/// The tier never inspects a buffered write: every question that depends on what a write means is
/// answered here, so the same machinery serves stores whose keys, versions and rows share nothing.
pub trait CommitDomain: Copy + Debug + 'static {
	/// Everything one store needs to answer a slice: the resident write set, the persistent tier it
	/// drains into, and the read tiers [`CommitDomain::settle`] must refresh. Opaque to the tier, which
	/// holds it and hands it back on every call, so the domain owns its own locking.
	type State: Send + Sync + 'static;

	/// One unit of work handed to [`CommitDomain::persist`]; the domain owns its shape and its lifetime.
	type Batch: Send + Sync + 'static;

	/// What a persist proved durable. `()` where every batch is accepted whole; a domain that can have
	/// part of a batch refused carries the accepted subset here, or [`CommitDomain::settle`] would
	/// release rows the persistent tier never took.
	type Ack: Send + Sync + 'static;

	/// What bounds a slice besides bytes. `()` where the domain flushes everything it holds.
	type Cutoff: Copy + Debug + Send + Sync + 'static;

	/// The pagination unit, and the unit starvation ordering ranks. `()` where the buffer is one run.
	type Kind: Copy + Eq + Debug + Send + Sync + 'static;

	const SCOPE: &'static str;

	/// The upper bound on how many slices one pagination may run before it stops and reports
	/// [`Progress::Yielded`]; without it a permanently full buffer never returns control to its host.
	const MAX_SLICES_PER_TICK: usize;

	/// The bound this slice may not pass, or `None` where nothing is currently evictable; a domain
	/// with no notion of a bound answers `Some(())`.
	fn cutoff(state: &Self::State) -> Option<Self::Cutoff>;

	/// The cutoff that admits everything, used by shutdown drain; a slice taken under it must leave
	/// the resident set empty, or shutdown loses data.
	fn cutoff_all() -> Self::Cutoff;

	/// The kinds holding pending work, in the order the domain wants them visited. The tier rotates
	/// this list to its resume cursor but never reorders it, so a domain that must protect a durable
	/// frontier returns its oldest-pending kind first.
	fn kinds(state: &Self::State) -> Vec<Self::Kind>;

	/// Take up to `budget` bytes of work from one kind. Returns the batch, the bytes it accounts for,
	/// and whether that kind still has work below the cutoff.
	///
	/// A slice must carry at least one entry when the kind has any, even one wider than the whole
	/// budget, or an oversized entry wedges the flush forever. Whether the rows leave the resident set
	/// or are borrowed is the domain's call; the tier only promises that [`CommitDomain::settle`] runs
	/// exactly once for every batch this returns.
	fn select(state: &Self::State, kind: Self::Kind, cutoff: Self::Cutoff, budget: ByteSize) -> Option<Slice<Self>>;

	/// Write one batch through the domain's own private transaction. Errors propagate; the tier never
	/// swallows one, so a failed persist stops the process rather than dropping the batch.
	fn persist(state: &Self::State, batch: &Self::Batch) -> reifydb_value::Result<Self::Ack>;

	/// Release what `ack` confirmed durable and refresh the read tiers. Must leave no row visible twice,
	/// or a reader sees a version the persistent tier already superseded.
	fn settle(state: &Self::State, batch: Self::Batch, ack: Self::Ack) -> Settlement;

	/// Bytes currently resident, read once per slice and once per metrics sample; must be O(1), or the
	/// budget check becomes the hot path.
	fn resident_bytes(state: &Self::State) -> ByteSize;

	fn kind_name(kind: Self::Kind) -> Cow<'static, str>;

	/// The resident footprint measured twice under one domain lock: the counter the budget carries, and
	/// the same set walked charge by charge, never merging a key two layers both hold and never dropping
	/// a tombstone's key charge. Called only from the assertion build. The two reads must be taken
	/// together, or a concurrent writer lands between them and fakes a drift that is not there.
	fn census(state: &Self::State) -> CommitCensus;

	/// Whether a resident set this small is worth one transaction. A domain that pays a fixed
	/// per-transaction cost refuses below its floor and waits for the tick instead; a drain ignores the
	/// refusal, since shutdown must empty the set whatever it costs.
	fn worth_persisting(bytes: ByteSize) -> bool {
		let _ = bytes;
		true
	}

	/// Whether a write may proceed while the resident set is over budget. Refusing turns the budget into
	/// backpressure; admitting turns it into a soft target and lets RAM overshoot.
	fn admits_over_budget_writes() -> bool {
		true
	}
}

/// The resident set measured two ways at one instant. Equality is the invariant: a counter that drifts
/// above the walk hides bytes the budget can never release, and one that drifts below lets the resident
/// set grow past a window that reports itself empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitCensus {
	pub counted: ByteSize,
	pub walked: ByteSize,
}

/// One unit of work the tier has taken from a kind but not yet settled.
pub struct Slice<D: CommitDomain> {
	pub batch: D::Batch,
	pub bytes: ByteSize,
	/// Whether this kind still holds work below the cutoff after this slice.
	pub more: bool,
}

/// What one settle released, so the tier can update the budget without re-scanning the resident set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Settlement {
	pub released: ByteSize,
	pub entries: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushOutcome {
	pub progress: Progress,
	pub slices: u64,
	pub persisted: u64,
	pub released: ByteSize,
	pub backlog: ByteSize,
}

impl FlushOutcome {
	pub fn exhausted() -> Self {
		Self {
			progress: Progress::Exhausted,
			slices: 0,
			persisted: 0,
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

/// How the tier asks its host to run a slice now. The tier owns no scheduler, so it does not know
/// whether the host is an actor or a lifecycle task.
pub trait CommitWaker: Send + Sync + 'static {
	fn wake(&self);
}

#[derive(Clone, Copy, Debug)]
pub struct CommitConfig {
	/// The collapse window: bytes the resident set may hold before a flush is triggered off the tick.
	pub budget: Option<ByteSize>,
	pub interval: Duration,
}

impl CommitConfig {
	/// A budget for tests only; production sizing comes from catalog config, never from a fallback here.
	pub fn testing() -> Self {
		Self {
			budget: Some(ByteSize::from_mib(4)),
			interval: Duration::from_seconds_const(5),
		}
	}
}

struct Inner<D: CommitDomain> {
	/// Set when a full-buffer wake has been raised and not yet consumed; without it a burst of writes
	/// raises one wake per write.
	triggered: bool,
	/// The kind a slice stopped short of; it is visited first next slice, or a hot kind ahead of it in
	/// the domain's order starves it forever.
	resume_from: Option<D::Kind>,
	metrics: CommitMetrics,
	kinds: Vec<(D::Kind, CommitMetrics)>,
}

struct Shared<D: CommitDomain> {
	state: D::State,
	inner: Mutex<Inner<D>>,
	/// Serialises flushes without holding any domain lock, so reads and writes proceed while the
	/// persistent tier works.
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
