// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::expiry::{ReapQueue, ReapQueueKey},
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::direction::Desc,
	},
};
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::resident::bucket::{BucketMap, FrozenRows, StandardBucket};

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

	pub fn retire_groups(&mut self, operator: OperatorId, groups: &[GroupId]) -> (usize, usize, ByteSize) {
		let (mut entries, mut tombstones, mut freed) = (0usize, 0usize, ByteSize::ZERO);
		for bucket in self.buckets.values_mut() {
			for group in groups {
				let (e, t, f) = bucket.retire_group(*group);
				entries += e;
				tombstones += t;
				freed = freed.saturating_add(f);
			}
		}
		if let Some(queue) = self.buckets.get_mut(&(operator, ReapQueue::ID)) {
			let queue = queue
				.as_any_mut()
				.downcast_mut::<StandardBucket<ReapQueue>>()
				.expect("a keyspace id must map to exactly one key type");
			for group in groups {
				let (e, f) = queue.forget_clean_row(
					GroupId::ROOT,
					&ReapQueueKey {
						group: Desc(*group),
					},
				);
				entries += e;
				freed = freed.saturating_add(f);
			}
		}
		self.buckets.retain(|_, bucket| !bucket.is_empty());
		(entries, tombstones, freed)
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
