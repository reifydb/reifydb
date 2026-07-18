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
	util::{
		budget::MemoryBudget,
		memory::{MemoryReporter, MemorySample},
	},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_store::row::page::PageId;
use reifydb_value::byte_size::ByteSize;

use crate::tier::read::{
	MultiReadBufferTier, OperatorReadBufferUsage, PoolInner, ReadBufferConfig, ReadBufferDomainConfig,
	ReadBufferWarmStats, Shard,
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

	pub fn operator_read_buffer_usage(&self) -> Vec<OperatorReadBufferUsage> {
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
		let mut out: Vec<OperatorReadBufferUsage> = usage_by_node
			.into_iter()
			.map(|(node, (resident, payload))| OperatorReadBufferUsage {
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

impl MemoryReporter for MultiReadBufferTier {
	fn report(&self, out: &mut Vec<MemorySample>) {
		out.push(MemorySample::new(
			"read_buffer::operator",
			"resident_bytes",
			self.operator_resident_bytes().as_bytes() as f64,
			"bytes",
		));
		out.push(MemorySample::new(
			"read_buffer::operator",
			"payload_bytes",
			self.operator_payload_bytes().as_bytes() as f64,
			"bytes",
		));
		out.push(MemorySample::new(
			"read_buffer::operator",
			"resident_pages",
			self.operator_resident_pages() as f64,
			"pages",
		));
		out.push(MemorySample::new(
			"read_buffer::general",
			"resident_bytes",
			self.general_resident_bytes().as_bytes() as f64,
			"bytes",
		));
		out.push(MemorySample::new(
			"read_buffer::general",
			"payload_bytes",
			self.general_payload_bytes().as_bytes() as f64,
			"bytes",
		));
		out.push(MemorySample::new(
			"read_buffer::general",
			"resident_pages",
			self.general_resident_pages() as f64,
			"pages",
		));
		for (scope, shards) in [
			("read_buffer::operator", &self.inner.operator_shards),
			("read_buffer::general", &self.inner.general_shards),
		] {
			let stats = domain_warm_stats(shards);
			out.push(MemorySample::new(scope, "warms_started", stats.warms_started as f64, "count"));
			out.push(MemorySample::new(scope, "warms_completed", stats.warms_completed as f64, "count"));
			out.push(MemorySample::new(
				scope,
				"warms_dirty_aborted",
				stats.warms_dirty_aborted as f64,
				"count",
			));
			out.push(MemorySample::new(scope, "warms_aborted", stats.warms_aborted as f64, "count"));
			out.push(MemorySample::new(
				scope,
				"pages_warm_blocked",
				stats.pages_warm_blocked as f64,
				"count",
			));
			out.push(MemorySample::new(scope, "pages_evicted", stats.pages_evicted as f64, "count"));
			out.push(MemorySample::new(
				scope,
				"complete_pages_invalidated",
				stats.complete_pages_invalidated as f64,
				"count",
			));
			out.push(MemorySample::new(
				scope,
				"shard_limit_bytes",
				shards[0].lock().budget.limit().as_bytes() as f64,
				"bytes",
			));
			for (index, shard) in shards.iter().enumerate() {
				out.push(MemorySample::new(
					format!("{scope}::shard::{index:02}"),
					"used_bytes",
					shard.lock().budget.used().as_bytes() as f64,
					"bytes",
				));
			}
		}
	}
}

fn domain_warm_stats(shards: &[Mutex<Shard>]) -> ReadBufferWarmStats {
	let mut total = ReadBufferWarmStats::default();
	for shard in shards {
		let stats = shard.lock().warm_stats;
		total.warms_started += stats.warms_started;
		total.warms_completed += stats.warms_completed;
		total.warms_dirty_aborted += stats.warms_dirty_aborted;
		total.warms_aborted += stats.warms_aborted;
		total.pages_warm_blocked += stats.pages_warm_blocked;
		total.pages_evicted += stats.pages_evicted;
		total.complete_pages_invalidated += stats.complete_pages_invalidated;
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
				warm_stats: ReadBufferWarmStats::default(),
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
				self.warm_stats.pages_evicted += 1;
			}
		}
	}
}
