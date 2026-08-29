// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod tests;

use std::{any::Any, collections::HashMap, ops::Bound};

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::state::{GroupId, KeyspaceId},
		typed::{ExclusiveUpperEnd, Key, range::KeyRange},
	},
};
use reifydb_store::{
	coverage::{
		cursor::{Cursor, ServedChunk},
		interval::Interval,
		plan::Segment,
	},
	tier::range::{Materialize, RangeConfig, RangeTier},
};
use reifydb_value::{Result, byte_size::ByteSize};
use rusqlite::Transaction;

use crate::tier::range::typed::{KeyspaceRow, TypedDomain, TypedPartition};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
	pub rows: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resume {
	Done,
	More,
}

pub trait AnyBucket: Any + Send + Sync {
	fn keyspace(&self) -> KeyspaceId;

	fn footprint(&self) -> ByteSize;

	fn flush(&mut self, tx: &mut Transaction<'_>) -> Result<()>;

	fn reap_group(&mut self, group: GroupId, budget: &mut Budget) -> Result<Resume>;

	fn as_any_mut(&mut self) -> &mut dyn Any;
}

pub struct TypedBucket<K: KeyspaceRow> {
	tier: RangeTier<TypedDomain<K>>,
}

impl<K: KeyspaceRow> TypedBucket<K> {
	pub fn new(shard_bytes: ByteSize) -> Option<Self> {
		let tier = RangeTier::new(RangeConfig {
			shard_bytes: Some(shard_bytes),
			shards: 1,
			..RangeConfig::testing()
		})?;
		Some(Self {
			tier,
		})
	}

	fn whole() -> KeyRange<K::Suffix> {
		KeyRange::new(Bound::Included(K::Suffix::low()), Bound::Unbounded)
	}

	fn partition(operator: OperatorId, group: GroupId) -> TypedPartition {
		TypedPartition {
			operator,
			group,
		}
	}

	pub fn insert(&self, operator: OperatorId, group: GroupId, suffix: K::Suffix, row: K::Row) {
		self.tier.insert_in(operator, Self::partition(operator, group), suffix, row)
	}

	pub fn overwrite(&self, operator: OperatorId, group: GroupId, suffix: K::Suffix, row: K::Row) {
		self.tier.overwrite_in(operator, Self::partition(operator, group), suffix, row)
	}

	pub fn mark_deleted(&self, operator: OperatorId, group: GroupId, suffix: &K::Suffix) {
		self.tier.mark_deleted_in(operator, Self::partition(operator, group), suffix)
	}

	pub fn get(&self, operator: OperatorId, group: GroupId, suffix: &K::Suffix) -> Option<Option<K::Row>> {
		self.tier.lookup_in(operator, Self::partition(operator, group), suffix)
	}

	pub fn group(&self, operator: OperatorId, group: GroupId, limit: usize) -> Option<Vec<(K::Suffix, K::Row)>> {
		let scan = self.tier.plan_scan_in(operator, Self::partition(operator, group), &Self::whole())?;
		let mut out = Vec::new();
		for segment in scan.segments() {
			let interval = match segment {
				Segment::Resident(interval) => interval,
				Segment::Gap {
					..
				} => return None,
			};
			let mut cursor = Cursor::new();
			while !cursor.is_exhausted() {
				match self.tier.serve(&scan, interval, &mut cursor, limit) {
					ServedChunk::Served(rows) => out.extend(rows),
					ServedChunk::Gap => return None,
				}
			}
		}
		Some(out)
	}

	pub fn materialize(&self, operator: OperatorId, group: GroupId, rows: &[(K::Suffix, K::Row)]) -> Materialize {
		let Some(scan) = self.tier.plan_scan_in(operator, Self::partition(operator, group), &Self::whole())
		else {
			return Materialize::Refused;
		};
		let span = Interval::new(K::Suffix::low(), ExclusiveUpperEnd::Top);
		self.tier.materialize(&scan, &span, rows)
	}

	pub fn invalidate_operator(&self, operator: OperatorId) {
		self.tier.invalidate_operator(operator)
	}

	pub fn clear(&self) {
		self.tier.clear()
	}
}

impl<K: KeyspaceRow> AnyBucket for TypedBucket<K> {
	fn keyspace(&self) -> KeyspaceId {
		K::ID
	}

	fn footprint(&self) -> ByteSize {
		self.tier.resident_bytes()
	}

	fn flush(&mut self, _tx: &mut Transaction<'_>) -> Result<()> {
		unimplemented!("per keyspace bucket flush arrives with the per keyspace sqlite tables at S8")
	}

	fn reap_group(&mut self, _group: GroupId, _budget: &mut Budget) -> Result<Resume> {
		unimplemented!("bucket reaping arrives with the store side reaper at S9")
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

#[derive(Default)]
pub struct BucketMap {
	buckets: HashMap<(OperatorId, KeyspaceId), Box<dyn AnyBucket>>,
}

impl BucketMap {
	pub fn bucket<K: KeyspaceRow>(&mut self, operator: OperatorId, shard_bytes: ByteSize) -> &mut TypedBucket<K> {
		self.buckets
			.entry((operator, K::ID))
			.or_insert_with(|| {
				Box::new(
					TypedBucket::<K>::new(shard_bytes)
						.expect("a bucket must be given a non zero shard budget"),
				)
			})
			.as_any_mut()
			.downcast_mut()
			.expect("a keyspace id must map to exactly one key type")
	}

	pub fn footprint(&self) -> ByteSize {
		ByteSize::from_bytes(self.buckets.values().map(|bucket| bucket.footprint().as_bytes()).sum())
	}
}
