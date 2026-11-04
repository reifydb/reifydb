// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator::state::KeyspaceId;
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::resident::bucket::{BucketMap, FrozenRows};

impl BucketMap {
	pub fn dirty_count(&self) -> usize {
		self.buckets.values().map(|bucket| bucket.dirty_len()).sum()
	}

	pub fn dirty_footprint(&self) -> ByteSize {
		self.buckets
			.values()
			.fold(ByteSize::ZERO, |total, bucket| total.saturating_add(bucket.dirty_footprint()))
	}

	pub fn settle_flushing(&mut self) {
		for bucket in self.buckets.values_mut() {
			bucket.settle_flushing();
		}
	}

	pub fn evict_clean(&mut self, bytes: &mut ByteSize, tombstones: &mut usize) -> (usize, ByteSize) {
		let mut evicted = 0usize;
		let mut freed = ByteSize::ZERO;
		for bucket in self.buckets.values_mut() {
			if bytes.as_bytes() == 0 && *tombstones == 0 {
				break;
			}
			let (count, released) = bucket.evict_clean(bytes, tombstones);
			evicted += count;
			freed = freed.saturating_add(released);
		}
		self.buckets.retain(|_, bucket| !bucket.is_empty());
		(evicted, freed)
	}

	#[instrument(name = "store::operator::bucket::freeze", level = "debug", skip_all)]
	pub fn freeze(&mut self) -> Frozen {
		let mut frozen = Frozen::default();
		for ((_, keyspace), bucket) in self.buckets.iter_mut() {
			if let Some((rows, count, bytes)) = bucket.freeze() {
				frozen.rows.push((*keyspace, rows));
				frozen.count += count;
				frozen.bytes = frozen.bytes.saturating_add(bytes);
			}
		}
		frozen
	}

	#[cfg(test)]
	pub fn flushing_count(&self) -> usize {
		self.buckets.values().map(|bucket| bucket.flushing_len()).sum()
	}
}

#[derive(Default)]
pub struct Frozen {
	pub rows: Vec<(KeyspaceId, Box<dyn FrozenRows>)>,
	pub count: usize,
	pub bytes: ByteSize,
}
