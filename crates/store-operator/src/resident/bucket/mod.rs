// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod bytes;
mod encoded;
mod flush;
mod range;
pub mod write;

#[cfg(test)]
mod tests;

use std::{any::Any, collections::HashMap, ops::Bound};

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		state::{GroupId, GroupStateKey, KeyspaceId},
		traits::Keyspace,
	},
};
use reifydb_value::byte_size::ByteSize;
use smallvec::SmallVec;

use crate::{
	bound::KeyspaceIds,
	resident::bucket::write::{StandardBucket, WriteEntry},
	types::Scan,
};

pub type GroupIds = SmallVec<[GroupId; 4]>;

pub trait Bucket: Any + Send + Sync {
	fn footprint(&self) -> ByteSize;

	fn len(&self) -> usize;

	fn dirty_len(&self) -> usize;

	fn dirty_footprint(&self) -> ByteSize;

	fn stage_dirty(&mut self, visit: &mut dyn FnMut(GroupId, &[u8], &WriteEntry)) -> ByteSize;

	fn revert_flushing(&mut self) -> usize;

	fn evict_clean(&mut self, bytes: &mut ByteSize, entries: &mut usize) -> (usize, ByteSize);

	fn settle_flushing(&mut self);

	fn is_empty(&self) -> bool {
		self.len() == 0
	}

	fn for_each(&self, visit: &mut dyn FnMut(GroupId, &[u8], &WriteEntry));

	fn groups_in_range(&self, lower: &Bound<GroupId>, upper: &Bound<GroupId>) -> GroupIds;

	fn encoded_range_in(
		&self,
		group: GroupId,
		start: &Bound<Vec<u8>>,
		end: &Bound<Vec<u8>>,
		scan: Scan,
		limit: usize,
		tombstones: bool,
	) -> Vec<(GroupStateKey, WriteEntry)>;

	fn as_any(&self) -> &dyn Any;

	fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Default)]
pub struct BucketMap {
	buckets: HashMap<(OperatorId, KeyspaceId), Box<dyn Bucket>>,
}

impl BucketMap {
	pub fn bucket<K: Keyspace>(&mut self, operator: OperatorId) -> &mut StandardBucket<K> {
		self.buckets
			.entry((operator, K::ID))
			.or_insert_with(|| Box::new(StandardBucket::<K>::new(operator)))
			.as_any_mut()
			.downcast_mut()
			.expect("a keyspace id must map to exactly one key type")
	}

	pub fn buckets(&self) -> impl Iterator<Item = (&(OperatorId, KeyspaceId), &dyn Bucket)> {
		self.buckets.iter().map(|(address, bucket)| (address, bucket.as_ref()))
	}

	pub fn keyspaces_of(&self, operator: OperatorId) -> KeyspaceIds {
		let mut ids: KeyspaceIds =
			self.buckets.keys().filter(|(id, _)| *id == operator).map(|(_, keyspace)| *keyspace).collect();
		ids.sort_by_key(|keyspace| keyspace.0);
		ids
	}

	pub fn entry_count(&self) -> usize {
		self.buckets.values().map(|bucket| bucket.len()).sum()
	}

	pub fn footprint(&self) -> ByteSize {
		ByteSize::from_bytes(self.buckets.values().map(|bucket| bucket.footprint().as_bytes()).sum())
	}
}
