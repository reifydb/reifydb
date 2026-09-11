// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, KeyspaceId},
};
use reifydb_value::byte_size::ByteSize;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use rusqlite::Transaction;
use tracing::instrument;

use crate::resident::bucket::{BucketMap, write::WriteEntry};

impl BucketMap {
	pub fn dirty_len(&self) -> usize {
		self.buckets.values().map(|bucket| bucket.dirty_len()).sum()
	}

	pub fn dirty_footprint(&self) -> ByteSize {
		self.buckets
			.values()
			.fold(ByteSize::ZERO, |total, bucket| total.saturating_add(bucket.dirty_footprint()))
	}

	pub fn revert_flushing(&mut self) -> usize {
		self.buckets.values_mut().map(|bucket| bucket.revert_flushing()).sum()
	}

	pub fn settle_flushing(&mut self) {
		for bucket in self.buckets.values_mut() {
			bucket.settle_flushing();
		}
	}

	pub fn evict_clean(&mut self, bytes: &mut ByteSize, entries: &mut usize) -> (usize, ByteSize) {
		let mut evicted = 0usize;
		let mut freed = ByteSize::ZERO;
		for bucket in self.buckets.values_mut() {
			if bytes.as_bytes() == 0 && *entries == 0 {
				break;
			}
			let (count, released) = bucket.evict_clean(bytes, entries);
			evicted += count;
			freed = freed.saturating_add(released);
		}
		self.buckets.retain(|_, bucket| !bucket.is_empty());
		(evicted, freed)
	}

	#[instrument(name = "store::operator::bucket::stage_dirty", level = "debug", skip_all, fields(operator = operator.0))]
	pub fn stage_dirty(
		&mut self,
		operator: OperatorId,
		mut visit: impl FnMut(KeyspaceId, GroupId, &[u8], &WriteEntry),
	) -> ByteSize {
		let mut staged = ByteSize::ZERO;
		let mut ids = self.keyspaces_of(operator);
		ids.reverse();
		for keyspace in ids {
			let Some(bucket) = self.buckets.get_mut(&(operator, keyspace)) else {
				continue;
			};
			staged =
				staged.saturating_add(bucket.stage_dirty(&mut |group, suffix, entry| {
					visit(keyspace, group, suffix, entry)
				}));
		}
		staged
	}

	pub fn absorb(&mut self, mut other: BucketMap) {
		for (address, mut bucket) in other.buckets.drain() {
			match self.buckets.get_mut(&address) {
				Some(existing) => existing.absorb_any(bucket.as_mut()),
				None => {
					self.buckets.insert(address, bucket);
				}
			}
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn write_into(&self, txn: &Transaction) {
		for bucket in self.buckets.values() {
			bucket.write_into(txn);
		}
	}
}
