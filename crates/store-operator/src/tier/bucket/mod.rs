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
use rusqlite::{Connection, Transaction};

use crate::{
	tier::bucket::write::{TypedBucket, WriteEntry},
	types::DurablePre,
};

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
pub struct BucketCensus {
	pub keyspace: KeyspaceId,
	pub keys: u64,
	pub key_bytes: ByteSize,
	pub value_bytes: ByteSize,
}

pub trait AnyBucket: Any + Send + Sync {
	fn keyspace(&self) -> KeyspaceId;

	fn footprint(&self) -> ByteSize;

	fn len(&self) -> usize;

	fn encoded_entries(&self) -> Vec<(EncodedKey, WriteEntry)>;

	fn census(&self) -> BucketCensus;

	fn absorb_any(&mut self, other: &mut dyn AnyBucket);

	fn as_any(&self) -> &dyn Any;

	fn flush(&mut self, conn: &Connection) -> Result<()>;

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
		durable_pre: DurablePre,
	) {
		struct Record<'a> {
			map: &'a mut BucketMap,
			operator: OperatorId,
			group: GroupId,
			suffix: &'a [u8],
			post: Option<EncodedPodRow>,
			durable_pre: DurablePre,
		}

		impl KeyspaceVisitor for Record<'_> {
			type Output = ();

			fn visit<K: Keyspace>(self) -> Self::Output {
				let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(self.suffix)
					.expect("a stored suffix must decode as its own keyspace's suffix type");
				self.map.bucket::<K>(self.operator).record(
					self.group,
					suffix,
					self.post,
					self.durable_pre,
				);
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
				durable_pre,
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

	pub fn census(&self, operator: OperatorId) -> Vec<BucketCensus> {
		let mut out: Vec<BucketCensus> = self
			.buckets
			.iter()
			.filter(|((id, _), _)| *id == operator)
			.map(|(_, bucket)| bucket.census())
			.filter(|census| census.keys > 0)
			.collect();
		out.sort_by_key(|census| census.keyspace.0);
		out
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

	pub fn encoded_range(
		&self,
		operator: OperatorId,
		lower: &Bound<EncodedKey>,
		upper: &Bound<EncodedKey>,
	) -> Vec<(EncodedKey, WriteEntry)> {
		let ids = match confined_keyspace(lower, upper) {
			Some(keyspace) => vec![keyspace],
			None => self.keyspaces_of(operator),
		};
		let mut out = Vec::new();
		for keyspace in ids {
			let Some(bucket) = self.buckets.get(&(operator, keyspace)) else {
				continue;
			};
			out.extend(bucket
				.encoded_entries()
				.into_iter()
				.filter(|(key, _)| in_bounds(key, lower, upper)));
		}
		out.sort_by(|(left, _), (right, _)| left.cmp(right));
		out
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
		self.get_bytes_ref(*operator, keyspace, group, &suffix)
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

	pub fn keyspaces_of(&self, operator: OperatorId) -> Vec<KeyspaceId> {
		let mut ids: Vec<KeyspaceId> =
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

fn address(bound: &Bound<EncodedKey>) -> Option<(GroupId, KeyspaceId)> {
	let (Bound::Included(key) | Bound::Excluded(key)) = bound else {
		return None;
	};
	OperatorStateKey::decode_inner(key.as_slice()).map(|(group, keyspace, _)| (group, keyspace))
}

fn confined_keyspace(lower: &Bound<EncodedKey>, upper: &Bound<EncodedKey>) -> Option<KeyspaceId> {
	let (group, keyspace) = address(lower)?;
	match address(upper) {
		Some((end_group, end_keyspace)) if end_group == group && end_keyspace == keyspace => Some(keyspace),
		Some(_) => None,
		None => None,
	}
}

fn in_bounds(key: &EncodedKey, lower: &Bound<EncodedKey>, upper: &Bound<EncodedKey>) -> bool {
	let above = match lower {
		Bound::Unbounded => true,
		Bound::Included(start) => key >= start,
		Bound::Excluded(start) => key > start,
	};
	let below = match upper {
		Bound::Unbounded => true,
		Bound::Included(end) => key <= end,
		Bound::Excluded(end) => key < end,
	};
	above && below
}

impl<'a> IntoIterator for &'a BucketMap {
	type Item = ((OperatorId, EncodedKey), WriteEntry);
	type IntoIter = std::vec::IntoIter<((OperatorId, EncodedKey), WriteEntry)>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter_encoded().into_iter()
	}
}
