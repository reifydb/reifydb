// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	mem::replace,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::byte_size::ByteSize;
use reifydb_wal::lsn::Lsn;

use crate::{
	common::CommitVersion,
	event::{EventListener, transaction::PostCommitEvent},
	interface::commit::CommitEntry,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};

pub enum CommitPull {
	Hit {
		entries: Vec<Arc<CommitEntry>>,
		advance_to: CommitVersion,
		more: bool,
	},

	Behind,
}

struct QueueInner {
	parked: BTreeMap<CommitVersion, (Lsn, Option<Arc<CommitEntry>>)>,
	entries: BTreeMap<CommitVersion, (u64, Arc<CommitEntry>)>,
	bytes: u64,
	cover_from: CommitVersion,
	released: CommitVersion,
	released_lsn: Lsn,
}

type Waker = Box<dyn Fn() + Send + Sync>;

struct QueueShared {
	inner: RwLock<QueueInner>,
	waker: RwLock<Option<Waker>>,
	armed: AtomicBool,
	limit: u64,
	wal_enabled: bool,
	released_through: AtomicU64,
	published_entries: AtomicU64,
	pull_hits: AtomicU64,
	pull_behinds: AtomicU64,
	evicted_floor: AtomicU64,
	evicted_ceiling: AtomicU64,
	over_soft_cap: AtomicU64,
}

#[derive(Clone)]
pub struct CommitQueue {
	shared: Arc<QueueShared>,
}

impl CommitQueue {
	pub fn new(limit: ByteSize, wal_enabled: bool, seed: CommitVersion) -> Self {
		Self {
			shared: Arc::new(QueueShared {
				inner: RwLock::new(QueueInner {
					parked: BTreeMap::new(),
					entries: BTreeMap::new(),
					bytes: 0,
					cover_from: seed,
					released: seed,
					released_lsn: Lsn::new(0),
				}),
				waker: RwLock::new(None),
				armed: AtomicBool::new(false),
				limit: limit.as_bytes().max(1),
				wal_enabled,
				released_through: AtomicU64::new(seed.0),
				published_entries: AtomicU64::new(0),
				pull_hits: AtomicU64::new(0),
				pull_behinds: AtomicU64::new(0),
				evicted_floor: AtomicU64::new(0),
				evicted_ceiling: AtomicU64::new(0),
				over_soft_cap: AtomicU64::new(0),
			}),
		}
	}

	pub fn set_waker(&self, waker: impl Fn() + Send + Sync + 'static) {
		*self.shared.waker.write() = Some(Box::new(waker));
	}

	pub fn disarm(&self) {
		self.shared.armed.store(false, Ordering::Release);
	}

	fn notify(&self) {
		if self.shared.armed.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok()
			&& let Some(waker) = self.shared.waker.read().as_ref()
		{
			waker();
		}
	}

	pub fn released(&self) -> (CommitVersion, Lsn) {
		let inner = self.shared.inner.read();
		(inner.released, inner.released_lsn)
	}

	pub fn released_through(&self) -> CommitVersion {
		CommitVersion(self.shared.released_through.load(Ordering::Acquire))
	}

	fn publish(&self, event: &PostCommitEvent) {
		let version = event.version().commit;
		let entry = CommitEntry::build(
			*event.version(),
			*event.lsn(),
			*event.at(),
			event.deltas().iter().cloned().collect(),
		);
		let entry = (!entry.deltas.is_empty()).then(|| Arc::new(entry));
		let mut inner = self.shared.inner.write();
		if version <= inner.released || inner.parked.contains_key(&version) {
			panic!(
				"the commit queue received version {} twice or at or below its released frontier {}, so \
				 a commit was published twice or the queue was seeded above a commit it never saw",
				version.0, inner.released.0
			);
		}
		inner.parked.insert(version, (*event.lsn(), entry));
		let mut released = false;
		loop {
			let next = next_version(inner.released);
			let Some((lsn, entry)) = inner.parked.remove(&next) else {
				break;
			};
			if let Some(entry) = entry {
				let bytes = entry.bytes();
				inner.bytes += bytes;
				inner.entries.insert(next, (bytes, entry));
				self.shared.published_entries.fetch_add(1, Ordering::Relaxed);
			}
			inner.released = next;
			inner.released_lsn = lsn;
			released = true;
		}
		if released {
			self.shared.released_through.store(inner.released.0, Ordering::Release);
			self.enforce_cap(&mut inner);
			drop(inner);
			self.notify();
		}
	}

	fn enforce_cap(&self, inner: &mut QueueInner) {
		if !self.shared.wal_enabled {
			if inner.bytes > self.shared.limit {
				self.shared.over_soft_cap.fetch_add(1, Ordering::Relaxed);
			}
			return;
		}
		while inner.bytes > self.shared.limit {
			let Some((version, (bytes, _))) = inner.entries.pop_first() else {
				break;
			};
			inner.bytes -= bytes;
			inner.cover_from = inner.cover_from.max(version);
			self.shared.evicted_ceiling.fetch_add(1, Ordering::Relaxed);
		}
	}

	pub fn pull(&self, cursor: CommitVersion, up_to: CommitVersion, budget: ByteSize) -> CommitPull {
		let inner = self.shared.inner.read();
		let up_to = up_to.min(inner.released);
		if up_to <= cursor {
			return CommitPull::Hit {
				entries: Vec::new(),
				advance_to: cursor,
				more: false,
			};
		}
		if cursor < inner.cover_from {
			self.shared.pull_behinds.fetch_add(1, Ordering::Relaxed);
			return CommitPull::Behind;
		}
		let budget = budget.as_bytes().max(1);
		let mut entries: Vec<Arc<CommitEntry>> = Vec::new();
		let mut taken = 0u64;
		let mut cut = false;
		for (bytes, entry) in inner.entries.range(next_version(cursor)..=up_to).map(|(_, held)| held) {
			if !entries.is_empty() && taken + bytes > budget {
				cut = true;
				break;
			}
			taken += bytes;
			entries.push(entry.clone());
		}
		self.shared.pull_hits.fetch_add(1, Ordering::Relaxed);
		if cut {
			let advance_to = entries.last().expect("a cut pull holds at least one entry").version.commit;
			return CommitPull::Hit {
				entries,
				advance_to,
				more: true,
			};
		}
		CommitPull::Hit {
			entries,
			advance_to: up_to,
			more: false,
		}
	}

	pub fn evict_below(&self, floor: CommitVersion) {
		let mut inner = self.shared.inner.write();
		let floor = floor.min(inner.released);
		let retained = inner.entries.split_off(&next_version(floor));
		let evicted = replace(&mut inner.entries, retained);
		inner.bytes -= evicted.values().map(|(bytes, _)| bytes).sum::<u64>();
		inner.cover_from = inner.cover_from.max(floor);
		self.shared.evicted_floor.fetch_add(evicted.len() as u64, Ordering::Relaxed);
	}
}

#[inline]
fn next_version(version: CommitVersion) -> CommitVersion {
	CommitVersion(version.0.saturating_add(1))
}

impl EventListener<PostCommitEvent> for CommitQueue {
	fn on(&self, event: &PostCommitEvent) {
		self.publish(event);
	}
}

impl MetricsCollector for CommitQueue {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let (bytes, entries) = {
			let inner = self.shared.inner.read();
			(inner.bytes, inner.entries.len() as u64)
		};
		let counter = |metric: &'static str, value: &AtomicU64| {
			MetricsSample::counter("commit_queue", metric, value.load(Ordering::Relaxed))
		};
		out.push(MetricsSample::heap("commit_queue", "bytes", ByteSize::from_bytes(bytes)));
		out.push(MetricsSample::count("commit_queue", "entries", entries));
		out.push(MetricsSample::count("commit_queue", "released_through", self.released_through().0));
		out.push(counter("published_entries", &self.shared.published_entries));
		out.push(counter("pull_hits", &self.shared.pull_hits));
		out.push(counter("pull_behinds", &self.shared.pull_behinds));
		out.push(counter("evicted_floor", &self.shared.evicted_floor));
		out.push(counter("evicted_ceiling", &self.shared.evicted_ceiling));
		out.push(counter("over_soft_cap", &self.shared.over_soft_cap));
	}
}
