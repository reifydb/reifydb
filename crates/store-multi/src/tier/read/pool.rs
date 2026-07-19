// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::{
		Arc,
		atomic::{AtomicU8, Ordering},
	},
};

use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, store::EntryKind},
	metrics::{collect::MetricsCollector, sample::MetricsSample},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store::row::page::PageId;
use reifydb_value::byte_size::ByteSize;

use crate::tier::read::{
	MultiReadBufferTier, PoolInner, ReadBufferConfig, ReadBufferDomainConfig, ReadBufferOperatorMetrics,
	ReadBufferReadMetrics, ReadBufferShardMetrics, ReadBufferStateMetrics, ReadBufferWarmMetrics, Shard,
};

impl MultiReadBufferTier {
	pub fn new(config: ReadBufferConfig) -> Self {
		Self {
			inner: Arc::new(PoolInner {
				operator_shards: build_shards(config.operator),
				general_shards: build_shards(config.general),
				bucket_shift: AtomicU8::new(config.bucket_shift),
			}),
		}
	}

	pub(super) fn bucket_shift(&self) -> u8 {
		self.inner.bucket_shift.load(Ordering::Relaxed)
	}

	pub(super) fn shard_for(&self, page: &PageId) -> &Mutex<Shard> {
		let shards = self.shards_for_kind(page.kind);
		let mut hasher = DefaultHasher::new();
		page.hash(&mut hasher);
		let index = (hasher.finish() % shards.len() as u64) as usize;
		&shards[index]
	}

	fn shards_for_kind(&self, kind: EntryKind) -> &[Mutex<Shard>] {
		if matches!(kind, EntryKind::Operator(_) | EntryKind::OperatorInternal(_)) {
			&self.inner.operator_shards
		} else {
			&self.inner.general_shards
		}
	}

	pub(super) fn all_shards(&self) -> impl Iterator<Item = &Mutex<Shard>> {
		self.inner.operator_shards.iter().chain(self.inner.general_shards.iter())
	}

	pub fn operator_resident_bytes(&self) -> ByteSize {
		domain_resident_bytes(&self.inner.operator_shards)
	}

	pub fn operator_resident_pages(&self) -> usize {
		domain_resident_pages(&self.inner.operator_shards)
	}

	pub fn operator_payload_bytes(&self) -> ByteSize {
		domain_payload_bytes(&self.inner.operator_shards)
	}

	pub fn general_resident_bytes(&self) -> ByteSize {
		domain_resident_bytes(&self.inner.general_shards)
	}

	pub fn general_resident_pages(&self) -> usize {
		domain_resident_pages(&self.inner.general_shards)
	}

	pub fn general_payload_bytes(&self) -> ByteSize {
		domain_payload_bytes(&self.inner.general_shards)
	}

	pub fn shard_metrics(&self) -> Vec<ReadBufferShardMetrics> {
		let mut out = Vec::with_capacity(self.inner.operator_shards.len() + self.inner.general_shards.len());
		for (domain, shards) in
			[("operator", &self.inner.operator_shards), ("general", &self.inner.general_shards)]
		{
			for (index, shard) in shards.iter().enumerate() {
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
					domain,
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
				});
			}
		}
		out
	}

	pub fn operator_metrics(&self) -> Vec<ReadBufferOperatorMetrics> {
		let mut usage_by_node: HashMap<FlowNodeId, (u64, u64)> = HashMap::new();
		for shard in self.inner.operator_shards.iter() {
			let shard = shard.lock();
			for (page_id, page) in &shard.pages {
				if let EntryKind::Operator(node) | EntryKind::OperatorInternal(node) = page_id.kind {
					let (resident, payload) = usage_by_node.entry(node).or_insert((0, 0));
					*resident += page.bytes as u64;
					*payload += page.payload as u64;
				}
			}
		}
		let mut out: Vec<ReadBufferOperatorMetrics> = usage_by_node
			.into_iter()
			.map(|(node, (resident, payload))| ReadBufferOperatorMetrics {
				node,
				resident: ByteSize::from_bytes(resident),
				payload: ByteSize::from_bytes(payload),
			})
			.collect();
		out.sort_by_key(|usage| usage.node);
		out
	}

	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.all_shards()
			.map(|shard| shard.lock().pages.values().map(|page| page.entries.len()).sum::<usize>())
			.sum()
	}

	#[cfg(test)]
	pub fn resident_pages(&self) -> usize {
		self.all_shards().map(|shard| shard.lock().pages.len()).sum()
	}

	#[cfg(test)]
	pub fn resident_bytes(&self) -> ByteSize {
		let total = self.all_shards().map(|shard| shard.lock().budget.used().as_bytes()).sum();
		ByteSize::from_bytes(total)
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
		out.push(MetricsSample::heap(
			"read_buffer::operator",
			"resident_bytes",
			self.operator_resident_bytes(),
		));
		out.push(MetricsSample::bytes("read_buffer::operator", "payload_bytes", self.operator_payload_bytes()));
		out.push(MetricsSample::count(
			"read_buffer::operator",
			"resident_pages",
			self.operator_resident_pages() as u64,
		));
		out.push(MetricsSample::heap("read_buffer::general", "resident_bytes", self.general_resident_bytes()));
		out.push(MetricsSample::bytes("read_buffer::general", "payload_bytes", self.general_payload_bytes()));
		out.push(MetricsSample::count(
			"read_buffer::general",
			"resident_pages",
			self.general_resident_pages() as u64,
		));
		for (scope, shards) in [
			("read_buffer::operator", &self.inner.operator_shards),
			("read_buffer::general", &self.inner.general_shards),
		] {
			let metrics = domain_warm_metrics(shards);
			out.push(MetricsSample::count(scope, "warms_started", metrics.warms_started as u64));
			out.push(MetricsSample::count(scope, "warms_completed", metrics.warms_completed as u64));
			out.push(MetricsSample::count(
				scope,
				"warms_dirty_aborted",
				metrics.warms_dirty_aborted as u64,
			));
			out.push(MetricsSample::count(scope, "warms_aborted", metrics.warms_aborted as u64));
			out.push(MetricsSample::count(scope, "pages_warm_blocked", metrics.pages_warm_blocked as u64));
			out.push(MetricsSample::count(scope, "pages_evicted", metrics.pages_evicted as u64));
			out.push(MetricsSample::count(
				scope,
				"complete_pages_invalidated",
				metrics.complete_pages_invalidated as u64,
			));
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
}

fn domain_warm_metrics(shards: &[Mutex<Shard>]) -> ReadBufferWarmMetrics {
	let mut total = ReadBufferWarmMetrics::default();
	for shard in shards {
		let metrics = shard.lock().warm_metrics;
		total.warms_started += metrics.warms_started;
		total.warms_completed += metrics.warms_completed;
		total.warms_dirty_aborted += metrics.warms_dirty_aborted;
		total.warms_aborted += metrics.warms_aborted;
		total.pages_warm_blocked += metrics.pages_warm_blocked;
		total.pages_evicted += metrics.pages_evicted;
		total.complete_pages_invalidated += metrics.complete_pages_invalidated;
	}
	total
}

fn domain_resident_bytes(shards: &[Mutex<Shard>]) -> ByteSize {
	let total = shards.iter().map(|shard| shard.lock().budget.used().as_bytes()).sum();
	ByteSize::from_bytes(total)
}

fn domain_payload_bytes(shards: &[Mutex<Shard>]) -> ByteSize {
	let total = shards
		.iter()
		.map(|shard| shard.lock().pages.values().map(|page| page.payload as u64).sum::<u64>())
		.sum();
	ByteSize::from_bytes(total)
}

fn domain_resident_pages(shards: &[Mutex<Shard>]) -> usize {
	shards.iter().map(|shard| shard.lock().pages.len()).sum()
}

fn build_shards(config: ReadBufferDomainConfig) -> Box<[Mutex<Shard>]> {
	let shard_count = config.shards.max(1);
	let page_cap = (config.resident_pages / shard_count).max(1);
	let byte_cap = ByteSize::from_bytes((config.resident_bytes.as_bytes() / shard_count as u64).max(1));
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

	pub(super) fn evict_to_capacity(&mut self) {
		while self.pages.len() > self.page_cap || self.budget.over_budget() {
			let Some(victim) = self.pick_victim() else {
				break;
			};
			if let Some(page) = self.pages.remove(&victim) {
				self.budget.release(ByteSize::from_bytes(page.bytes as u64));
				self.warm_metrics.pages_evicted += 1;
			}
		}
	}
}
