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
	floors: BTreeMap<u64, CommitVersion>,
}

type Waker = Box<dyn Fn() + Send + Sync>;

struct FloorWaker {
	armed: AtomicBool,
	wake: Option<Waker>,
}

struct QueueShared {
	inner: RwLock<QueueInner>,
	wakers: RwLock<BTreeMap<u64, FloorWaker>>,
	next_floor: AtomicU64,
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
					floors: BTreeMap::new(),
				}),
				wakers: RwLock::new(BTreeMap::new()),
				next_floor: AtomicU64::new(0),
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

	pub fn floor(&self, name: &str) -> CommitQueueFloor {
		let id = self.shared.next_floor.fetch_add(1, Ordering::Relaxed);
		{
			let mut inner = self.shared.inner.write();
			let held = inner.released;
			inner.floors.insert(id, held);
		}
		self.shared.wakers.write().insert(
			id,
			FloorWaker {
				armed: AtomicBool::new(false),
				wake: None,
			},
		);
		CommitQueueFloor {
			handle: Arc::new(FloorHandle {
				queue: self.clone(),
				id,
				name: name.to_string(),
			}),
		}
	}

	fn notify(&self) {
		for waker in self.shared.wakers.read().values() {
			if let Some(wake) = waker.wake.as_ref()
				&& waker.armed
					.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
					.is_ok()
			{
				wake();
			}
		}
	}

	fn free_below_floors(&self, inner: &mut QueueInner) {
		let floor = inner.floors.values().min().copied().unwrap_or(inner.released).min(inner.released);
		let retained = inner.entries.split_off(&next_version(floor));
		let freed = replace(&mut inner.entries, retained);
		inner.bytes -= freed.values().map(|(bytes, _)| bytes).sum::<u64>();
		inner.cover_from = inner.cover_from.max(floor);
		self.shared.evicted_floor.fetch_add(freed.len() as u64, Ordering::Relaxed);
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
			self.free_below_floors(&mut inner);
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
}

struct FloorHandle {
	queue: CommitQueue,
	id: u64,
	name: String,
}

impl Drop for FloorHandle {
	fn drop(&mut self) {
		self.queue.shared.wakers.write().remove(&self.id);
		let mut inner = self.queue.shared.inner.write();
		inner.floors.remove(&self.id);
		self.queue.free_below_floors(&mut inner);
	}
}

#[derive(Clone)]
pub struct CommitQueueFloor {
	handle: Arc<FloorHandle>,
}

impl CommitQueueFloor {
	pub fn adopt(&self, start: CommitVersion, waker: impl Fn() + Send + Sync + 'static) {
		let handle = &self.handle;
		let shared = &handle.queue.shared;
		{
			let mut inner = shared.inner.write();
			if start < inner.cover_from {
				panic!(
					"commit queue floor {} adopted at version {} but the queue only holds commits after {}, \
					 so the commits between were freed before their reader took this floor",
					handle.name, start.0, inner.cover_from.0
				);
			}
			inner.floors.insert(handle.id, start);
			handle.queue.free_below_floors(&mut inner);
		}
		if let Some(slot) = shared.wakers.write().get_mut(&handle.id) {
			slot.wake = Some(Box::new(waker));
		}
	}

	pub fn advance(&self, version: CommitVersion) {
		let handle = &self.handle;
		let mut inner = handle.queue.shared.inner.write();
		let target = version.min(inner.released);
		let Some(held) = inner.floors.get_mut(&handle.id) else {
			return;
		};
		if target <= *held {
			return;
		}
		*held = target;
		handle.queue.free_below_floors(&mut inner);
	}

	pub fn disarm(&self) {
		if let Some(slot) = self.handle.queue.shared.wakers.read().get(&self.handle.id) {
			slot.armed.store(false, Ordering::Release);
		}
	}
}

#[derive(Clone)]
pub struct FlowQueueFloor(pub CommitQueueFloor);

#[derive(Clone)]
pub struct SubscriptionQueueFloor(pub CommitQueueFloor);

pub fn lsn_through(
	entries: &[Arc<CommitEntry>],
	through: CommitVersion,
	floor: Lsn,
	frontier: (CommitVersion, Lsn),
) -> Lsn {
	let lsn = entries
		.iter()
		.filter(|entry| entry.version.commit <= through)
		.map(|entry| entry.lsn)
		.fold(floor, Lsn::max);
	if through >= frontier.0 {
		lsn.max(frontier.1)
	} else {
		lsn
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
