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

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		state::{GroupId, GroupStateKey, KeyspaceId},
		traits::Keyspace,
	},
};
use reifydb_value::{Result, byte_size::ByteSize};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use rusqlite::{Connection, Transaction};
use smallvec::SmallVec;

use crate::{
	bound::KeyspaceIds,
	resident::bucket::write::{TypedBucket, WriteEntry},
	types::{Budget, Resume, Scan},
};

pub type GroupIds = SmallVec<[GroupId; 4]>;

pub trait AnyBucket: Any + Send + Sync {
	fn keyspace(&self) -> KeyspaceId;

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

	fn encoded_entries(&self) -> Vec<(EncodedKey, WriteEntry)>;

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

	fn absorb_any(&mut self, other: &mut dyn AnyBucket);

	fn as_any(&self) -> &dyn Any;

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	fn flush(&mut self, conn: &Connection) -> Result<()>;

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	fn write_into(&self, txn: &Transaction);

	fn reap_group(&mut self, group: GroupId, budget: &mut Budget) -> Result<Resume>;

	fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Default)]
pub struct BucketMap {
	buckets: HashMap<(OperatorId, KeyspaceId), Box<dyn AnyBucket>>,
}

impl BucketMap {
	pub fn bucket<K: Keyspace>(&mut self, operator: OperatorId) -> &mut TypedBucket<K> {
		self.buckets
			.entry((operator, K::ID))
			.or_insert_with(|| Box::new(TypedBucket::<K>::new(operator)))
			.as_any_mut()
			.downcast_mut()
			.expect("a keyspace id must map to exactly one key type")
	}

	pub fn operators(&self) -> Vec<OperatorId> {
		let mut ids: Vec<OperatorId> = self.buckets.keys().map(|(operator, _)| *operator).collect();
		ids.sort_by_key(|operator| operator.0);
		ids.dedup();
		ids
	}

	pub fn any(&mut self, operator: OperatorId, keyspace: KeyspaceId) -> Option<&mut dyn AnyBucket> {
		self.buckets.get_mut(&(operator, keyspace)).map(|bucket| bucket.as_mut())
	}

	pub fn iter_mut(&mut self) -> impl Iterator<Item = (&(OperatorId, KeyspaceId), &mut Box<dyn AnyBucket>)> {
		self.buckets.iter_mut()
	}

	pub fn keyspaces_of(&self, operator: OperatorId) -> KeyspaceIds {
		let mut ids: KeyspaceIds =
			self.buckets.keys().filter(|(id, _)| *id == operator).map(|(_, keyspace)| *keyspace).collect();
		ids.sort_by_key(|keyspace| keyspace.0);
		ids
	}

	pub fn remove_operator(&mut self, operator: OperatorId) {
		self.buckets.retain(|(id, _), _| *id != operator);
	}

	pub fn len(&self) -> usize {
		self.buckets.values().map(|bucket| bucket.len()).sum()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn footprint(&self) -> ByteSize {
		ByteSize::from_bytes(self.buckets.values().map(|bucket| bucket.footprint().as_bytes()).sum())
	}
}
