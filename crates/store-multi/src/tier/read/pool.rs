// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::{Arc, atomic::AtomicU64},
};

use reifydb_core::{
	metrics::{collect::MetricsCollector, sample::MetricsSample},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::{mutex::Mutex, rwlock::RwLock};
use reifydb_store::row::page::PageId;
use reifydb_value::byte_size::ByteSize;

#[cfg(test)]
use crate::tier::read::FillInterlock;
use crate::tier::read::{
	CoverageIndex, MultiReadBufferTier, PoolInner, ReadBufferConfig, ReadBufferCoverageMetrics,
	ReadBufferReadMetrics, ReadBufferShardMetrics, ReadBufferStateMetrics, ReadBufferWarmMetrics, Shard, Span,
};

const READ_BUFFER_SCOPE: &str = "read_buffer";

impl MultiReadBufferTier {
	pub fn new(config: ReadBufferConfig) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				bucket_shift: config.bucket_shift,
				coverage: RwLock::new(CoverageIndex {
					kinds: HashMap::new(),
				}),
				retractions: AtomicU64::new(0),
				fill_sequence: AtomicU64::new(0),
				#[cfg(test)]
				claims_published: AtomicU64::new(0),
				#[cfg(test)]
				claims_refused: AtomicU64::new(0),
				#[cfg(test)]
				drops_refused: AtomicU64::new(0),
				#[cfg(test)]
				interlock: None,
			}),
		})
	}

	#[cfg(test)]
	pub(super) fn with_interlock(config: ReadBufferConfig, interlock: FillInterlock) -> Option<Self> {
		let resident_bytes = config.resident_bytes?;
		Some(Self {
			inner: Arc::new(PoolInner {
				shards: build_shards(config, resident_bytes),
				bucket_shift: config.bucket_shift,
				coverage: RwLock::new(CoverageIndex {
					kinds: HashMap::new(),
				}),
				retractions: AtomicU64::new(0),
				fill_sequence: AtomicU64::new(0),
				claims_published: AtomicU64::new(0),
				claims_refused: AtomicU64::new(0),
				drops_refused: AtomicU64::new(0),
				interlock: Some(interlock),
			}),
		})
	}

	#[cfg(test)]
	pub(super) fn interlock(&self, page: PageId) {
		if let Some(interlock) = self.inner.interlock.as_ref() {
			interlock(self, page);
		}
	}

	pub(super) fn bucket_shift(&self) -> u8 {
		self.inner.bucket_shift
	}

	/// The next never-reused fill number, stamped on a page by the fill that widened its hull.
	///
	/// It must be tier-wide rather than per page: a per-page count restarts at one on a recreated
	/// page, which is exactly the value a stale evictor is still holding from before the drop. At one
	/// draw per nanosecond a `u64` needs 584 years to wrap, so a collision cannot arise in practice.
	pub(super) fn next_fill(&self) -> u64 {
		self.inner.fill_sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
	}

	pub(super) fn shard_index(&self, page: &PageId) -> usize {
		let mut hasher = DefaultHasher::new();
		page.hash(&mut hasher);
		(hasher.finish() % self.inner.shards.len() as u64) as usize
	}

	pub(super) fn shard(&self, index: usize) -> &Mutex<Shard> {
		&self.inner.shards[index]
	}

	pub(super) fn shard_for(&self, page: &PageId) -> &Mutex<Shard> {
		self.shard(self.shard_index(page))
	}

	/// Drops pages until the shard fits, retracting each victim's claims before its rows leave RAM.
	///
	/// The two locks are taken in turn, never together, so a fill can slip between the shrink and the
	/// drop; the fill count is re-read to catch exactly that, since dropping then would leave the
	/// fill's fresh claim standing over a page that is gone.
	pub(super) fn evict_to_capacity(&self, index: usize) {
		loop {
			let Some((victim, hull, fills)) = self.pick_victim(index) else {
				break;
			};
			if let Some(hull) = hull {
				self.withdraw_span(victim.kind, &hull);
			}
			if !self.drop_victim(index, victim, fills) {
				break;
			}
		}
	}

	pub(super) fn pick_victim(&self, index: usize) -> Option<(PageId, Option<Span>, u64)> {
		let shard = self.shard(index).lock();
		if shard.pages.len() <= shard.page_cap && !shard.budget.over_budget() {
			return None;
		}
		let victim = shard.pick_victim()?;
		let page = shard.pages.get(&victim)?;
		Some((victim, page.claimed.clone(), page.fills))
	}

	pub(super) fn drop_victim(&self, index: usize, victim: PageId, fills: u64) -> bool {
		let mut shard = self.shard(index).lock();
		match shard.pages.get(&victim) {
			Some(page) if page.fills != fills => {
				#[cfg(test)]
				self.inner.drops_refused.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
				return false;
			}
			Some(_) => {}
			None => return true,
		}
		if let Some(page) = shard.pages.remove(&victim) {
			shard.budget.release(ByteSize::from_bytes(page.bytes as u64));
			shard.warm_metrics.pages_evicted += 1;
		}
		true
	}

	/// Retracts and drops a page a removal has emptied, so no claim outlives the rows that backed it.
	pub(super) fn retract_page(&self, index: usize, victim: PageId, keep_complete: bool) {
		let removable = |shard: &Shard| {
			shard.pages
				.get(&victim)
				.is_some_and(|page| page.entries.is_empty() && !(keep_complete && page.range_complete))
		};
		let (hull, fills) = {
			let shard = self.shard(index).lock();
			if !removable(&shard) {
				return;
			}
			let page = shard.pages.get(&victim).expect("a removable page is present under the lock");
			(page.claimed.clone(), page.fills)
		};
		if let Some(hull) = hull {
			self.withdraw_span(victim.kind, &hull);
		}
		let mut shard = self.shard(index).lock();
		if !removable(&shard) || shard.pages.get(&victim).is_none_or(|page| page.fills != fills) {
			return;
		}
		if let Some(page) = shard.pages.remove(&victim) {
			shard.budget.release(ByteSize::from_bytes(page.bytes as u64));
		}
	}

	pub(super) fn all_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
		self.inner.shards.iter()
	}

	pub fn resident_bytes(&self) -> ByteSize {
		total_resident_bytes(&self.inner.shards)
	}

	pub fn resident_pages(&self) -> usize {
		total_resident_pages(&self.inner.shards)
	}

	pub fn payload_bytes(&self) -> ByteSize {
		total_payload_bytes(&self.inner.shards)
	}

	pub fn shard_metrics(&self) -> Vec<ReadBufferShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.shards.len());
		for (index, shard) in self.inner.shards.iter().enumerate() {
			let shard = shard.lock();
			let mut payload = 0u64;
			let mut entries = 0usize;
			let mut hot_pages = 0usize;
			let mut complete_pages = 0usize;
			let mut blocked_pages = 0usize;
			for page in shard.pages.values() {
				payload += page.payload as u64;
				entries += page.entries.len();
				hot_pages += usize::from(page.hot);
				complete_pages += usize::from(page.range_complete);
				blocked_pages += usize::from(page.warm_blocked);
			}
			out.push(ReadBufferShardMetrics {
				shard: index,
				state: ReadBufferStateMetrics {
					used: shard.budget.used(),
					limit: shard.budget.limit(),
					pages: shard.pages.len(),
					page_cap: shard.page_cap,
					payload: ByteSize::from_bytes(payload),
					entries,
					hot_pages,
					complete_pages,
					blocked_pages,
					warming: shard.warming.len(),
				},
				warms: shard.warm_metrics,
				reads: shard.read_metrics,
				coverage: shard.coverage_metrics,
			});
		}
		out
	}

	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.all_shards()
			.map(|shard| shard.lock().pages.values().map(|page| page.entries.len()).sum::<usize>())
			.sum()
	}

	#[cfg(test)]
	pub fn tallied_page_bytes(&self) -> ByteSize {
		let total = self
			.all_shards()
			.map(|shard| shard.lock().pages.values().map(|page| page.bytes as u64).sum::<u64>())
			.sum();
		ByteSize::from_bytes(total)
	}
}

impl MetricsCollector for MultiReadBufferTier {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::heap(READ_BUFFER_SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::bytes(READ_BUFFER_SCOPE, "payload_bytes", self.payload_bytes()));
		out.push(MetricsSample::count(READ_BUFFER_SCOPE, "resident_pages", self.resident_pages() as u64));
		let scope = READ_BUFFER_SCOPE;
		let shards = &self.inner.shards;
		out.push(MetricsSample::bytes(scope, "shard_limit_bytes", shards[0].lock().budget.limit()));
		for (index, shard) in shards.iter().enumerate() {
			out.push(MetricsSample::bytes(
				format!("{scope}::shard::{index:02}"),
				"used_bytes",
				shard.lock().budget.used(),
			));
		}
	}
}

fn total_resident_bytes(shards: &[Mutex<Shard>]) -> ByteSize {
	let total = shards.iter().map(|shard| shard.lock().budget.used().as_bytes()).sum();
	ByteSize::from_bytes(total)
}

fn total_payload_bytes(shards: &[Mutex<Shard>]) -> ByteSize {
	let total = shards
		.iter()
		.map(|shard| shard.lock().pages.values().map(|page| page.payload as u64).sum::<u64>())
		.sum();
	ByteSize::from_bytes(total)
}

fn total_resident_pages(shards: &[Mutex<Shard>]) -> usize {
	shards.iter().map(|shard| shard.lock().pages.len()).sum()
}

fn build_shards(config: ReadBufferConfig, resident_bytes: ByteSize) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let page_cap = (config.resident_pages / shard_count).max(1);
	let byte_cap = ByteSize::from_bytes((resident_bytes.as_bytes() / shard_count as u64).max(1));
	(0..shard_count)
		.map(|_| {
			Mutex::new(Shard {
				pages: HashMap::new(),
				warming: HashMap::new(),
				next_tick: 0,
				page_cap,
				budget: MemoryBudget::new(byte_cap),
				warm_metrics: ReadBufferWarmMetrics::default(),
				read_metrics: ReadBufferReadMetrics::default(),
				coverage_metrics: ReadBufferCoverageMetrics::default(),
			})
		})
		.collect::<Vec<_>>()
		.into_boxed_slice()
}

impl Shard {
	fn pick_victim(&self) -> Option<PageId> {
		let mut probationary: Option<(u64, PageId)> = None;
		let mut hot: Option<(u64, PageId)> = None;
		for (id, page) in &self.pages {
			let slot = if page.hot {
				&mut hot
			} else {
				&mut probationary
			};
			if slot.map(|(tick, _)| page.tick < tick).unwrap_or(true) {
				*slot = Some((page.tick, *id));
			}
		}
		probationary.or(hot).map(|(_, id)| id)
	}
}
