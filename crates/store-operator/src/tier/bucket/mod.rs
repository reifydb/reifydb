// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod write;

#[cfg(test)]
mod tests;

use std::{any::Any, collections::HashMap, ops::Bound};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::{KeyspaceVisitor, dispatch},
		state::{GroupId, KeyspaceId, OperatorStateKey},
		traits::Keyspace,
	},
	state::typed::SuffixBytes,
};
use reifydb_value::{Result, byte_size::ByteSize};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use rusqlite::{Connection, Transaction};
use smallvec::SmallVec;

use crate::tier::{
	bound::{KeyspaceIds, span, split_bound},
	bucket::write::{TypedBucket, WriteEntry},
};

pub type GroupIds = SmallVec<[GroupId; 4]>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
	pub rows: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resume {
	Done,
	More,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scan {
	Forward,
	Backward,
}

pub trait AnyBucket: Any + Send + Sync {
	fn keyspace(&self) -> KeyspaceId;

	fn footprint(&self) -> ByteSize;

	fn len(&self) -> usize;

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
	) -> Vec<(EncodedKey, WriteEntry)>;

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

	pub fn record_bytes(
		&mut self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		suffix: &[u8],
		post: Option<EncodedPodRow>,
	) {
		struct Record<'a> {
			map: &'a mut BucketMap,
			operator: OperatorId,
			group: GroupId,
			suffix: &'a [u8],
			post: Option<EncodedPodRow>,
		}

		impl KeyspaceVisitor for Record<'_> {
			type Output = ();

			fn visit<K: Keyspace>(self) -> Self::Output {
				let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(self.suffix)
					.expect("a stored suffix must decode as its own keyspace's suffix type");
				self.map.bucket::<K>(self.operator).record(self.group, suffix, self.post);
			}
		}

		dispatch(
			keyspace,
			Record {
				map: self,
				operator,
				group,
				suffix,
				post,
			},
		)
		.expect("a write must name a keyspace the catalogue declares");
	}

	pub fn get_bytes(
		&mut self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		suffix: &[u8],
	) -> Option<WriteEntry> {
		struct Get<'a> {
			map: &'a mut BucketMap,
			operator: OperatorId,
			group: GroupId,
			suffix: &'a [u8],
		}

		impl KeyspaceVisitor for Get<'_> {
			type Output = Option<WriteEntry>;

			fn visit<K: Keyspace>(self) -> Self::Output {
				let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(self.suffix)?;
				self.map.bucket::<K>(self.operator).get(self.group, &suffix).cloned()
			}
		}

		dispatch(
			keyspace,
			Get {
				map: self,
				operator,
				group,
				suffix,
			},
		)
		.flatten()
	}

	pub fn page_bytes(
		&mut self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		from: Bound<Vec<u8>>,
		until: Bound<Vec<u8>>,
		limit: Option<usize>,
	) -> Vec<(Vec<u8>, WriteEntry)> {
		struct Page<'a> {
			map: &'a mut BucketMap,
			operator: OperatorId,
			group: GroupId,
			from: Bound<Vec<u8>>,
			until: Bound<Vec<u8>>,
			limit: Option<usize>,
		}

		fn decode<S: SuffixBytes>(bound: Bound<Vec<u8>>) -> Bound<S> {
			match bound {
				Bound::Unbounded => Bound::Unbounded,
				Bound::Included(bytes) => {
					S::from_suffix_bytes(&bytes).map_or(Bound::Unbounded, Bound::Included)
				}
				Bound::Excluded(bytes) => {
					S::from_suffix_bytes(&bytes).map_or(Bound::Unbounded, Bound::Excluded)
				}
			}
		}

		impl KeyspaceVisitor for Page<'_> {
			type Output = Vec<(Vec<u8>, WriteEntry)>;

			fn visit<K: Keyspace>(self) -> Self::Output {
				let bounds = (decode::<K::Suffix>(self.from), decode::<K::Suffix>(self.until));
				let rows =
					self.map.bucket::<K>(self.operator)
						.range(self.group, bounds)
						.map(|(suffix, entry)| (suffix.to_suffix_bytes(), entry.clone()));
				match self.limit {
					Some(limit) => rows.take(limit).collect(),
					None => rows.collect(),
				}
			}
		}

		dispatch(
			keyspace,
			Page {
				map: self,
				operator,
				group,
				from,
				until,
				limit,
			},
		)
		.unwrap_or_default()
	}

	pub fn for_each_entry(
		&self,
		operator: OperatorId,
		mut visit: impl FnMut(KeyspaceId, GroupId, &[u8], &WriteEntry),
	) {
		let mut ids = self.keyspaces_of(operator);
		ids.reverse();
		for keyspace in ids {
			let Some(bucket) = self.buckets.get(&(operator, keyspace)) else {
				continue;
			};
			bucket.for_each(&mut |group, suffix, entry| visit(keyspace, group, suffix, entry));
		}
	}

	pub fn encoded_entries(&self, operator: OperatorId) -> Vec<(EncodedKey, WriteEntry)> {
		let mut ids = self.keyspaces_of(operator);
		ids.reverse();
		let mut out = Vec::new();
		for keyspace in ids {
			let Some(bucket) = self.buckets.get(&(operator, keyspace)) else {
				continue;
			};
			out.extend(bucket.encoded_entries());
		}
		out.sort_by(|(left, _), (right, _)| left.cmp(right));
		out
	}

	pub fn operators(&self) -> Vec<OperatorId> {
		let mut ids: Vec<OperatorId> = self.buckets.keys().map(|(operator, _)| *operator).collect();
		ids.sort_by_key(|operator| operator.0);
		ids.dedup();
		ids
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

	pub fn get_bytes_ref(
		&self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		suffix: &[u8],
	) -> Option<WriteEntry> {
		struct Get<'a> {
			bucket: &'a dyn AnyBucket,
			group: GroupId,
			suffix: &'a [u8],
		}

		impl KeyspaceVisitor for Get<'_> {
			type Output = Option<WriteEntry>;

			fn visit<K: Keyspace>(self) -> Self::Output {
				let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(self.suffix)?;
				self.bucket
					.as_any()
					.downcast_ref::<TypedBucket<K>>()
					.expect("a keyspace id must map to exactly one key type")
					.get(self.group, &suffix)
					.cloned()
			}
		}

		let bucket = self.buckets.get(&(operator, keyspace))?;
		dispatch(
			keyspace,
			Get {
				bucket: bucket.as_ref(),
				group,
				suffix,
			},
		)
		.flatten()
	}

	fn groups_of(&self, operator: OperatorId, lower: Bound<GroupId>, upper: Bound<GroupId>) -> GroupIds {
		let mut ids = GroupIds::new();
		for keyspace in self.keyspaces_of(operator) {
			let Some(bucket) = self.buckets.get(&(operator, keyspace)) else {
				continue;
			};
			ids.extend(bucket.groups_in_range(&lower, &upper));
		}
		ids.sort_by(|left, right| right.0.cmp(&left.0));
		ids.dedup();
		ids
	}

	pub fn encoded_range(
		&self,
		operator: OperatorId,
		lower: &Bound<EncodedKey>,
		upper: &Bound<EncodedKey>,
		scan: Scan,
		limit: usize,
	) -> Vec<(EncodedKey, WriteEntry)> {
		let (start, start_group, start_at) = split_bound(lower.as_ref());
		let (end, end_group, end_at) = split_bound(upper.as_ref());
		let end_open = matches!(end, Bound::Excluded(ref suffix) if suffix.is_empty());

		if let (Some(low), Some(high)) = (end_group, start_group)
			&& low > high
		{
			return Vec::new();
		}
		let lower = end_group.map_or(Bound::Unbounded, Bound::Included);
		let upper = start_group.map_or(Bound::Unbounded, Bound::Included);

		let mut groups = self.groups_of(operator, lower, upper);
		if scan == Scan::Backward {
			groups.reverse();
		}

		let mut chunks: SmallVec<[Vec<(EncodedKey, WriteEntry)>; 4]> = SmallVec::new();
		let mut taken = 0usize;
		'groups: for group in groups {
			let opens = start_group == Some(group);
			let closes = end_group == Some(group);
			let mut ids = span(
				opens.then_some(start_at).flatten(),
				closes.then_some(end_at).flatten(),
				closes && end_open,
			);
			if scan == Scan::Backward {
				ids.reverse();
			}
			for keyspace in ids {
				let Some(bucket) = self.buckets.get(&(operator, keyspace)) else {
					continue;
				};
				let from = match opens && start_at == Some(keyspace) {
					true => start.clone(),
					false => Bound::Unbounded,
				};
				let to = match closes && end_at == Some(keyspace) {
					true => end.clone(),
					false => Bound::Unbounded,
				};
				let chunk = bucket.encoded_range_in(group, &from, &to, scan, limit - taken);
				taken += chunk.len();
				chunks.push(chunk);
				if taken >= limit {
					break 'groups;
				}
			}
		}
		if scan == Scan::Backward {
			chunks.reverse();
		}
		chunks.into_iter().flatten().collect()
	}

	pub fn iter_encoded(&self) -> Vec<((OperatorId, EncodedKey), WriteEntry)> {
		let mut out = Vec::new();
		for operator in self.operators() {
			out.extend(self
				.encoded_entries(operator)
				.into_iter()
				.map(|(key, entry)| ((operator, key), entry)));
		}
		out
	}

	pub fn get(&self, address: &(OperatorId, EncodedKey)) -> Option<WriteEntry> {
		let (operator, key) = address;
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		self.get_bytes_ref(*operator, keyspace, group, suffix)
	}

	pub fn contains_key(&self, address: &(OperatorId, EncodedKey)) -> bool {
		self.get(address).is_some()
	}

	pub fn iter(&self) -> impl Iterator<Item = ((OperatorId, EncodedKey), WriteEntry)> {
		self.iter_encoded().into_iter()
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

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn write_into(&self, txn: &Transaction) {
		for bucket in self.buckets.values() {
			bucket.write_into(txn);
		}
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

impl IntoIterator for &BucketMap {
	type Item = ((OperatorId, EncodedKey), WriteEntry);
	type IntoIter = std::vec::IntoIter<((OperatorId, EncodedKey), WriteEntry)>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter_encoded().into_iter()
	}
}
