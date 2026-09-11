// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::{KeyspaceVisitor, dispatch},
		state::{GroupId, KeyspaceId},
		traits::Keyspace,
	},
	state::typed::SuffixBytes,
};

use crate::resident::bucket::{
	AnyBucket, BucketMap,
	write::{TypedBucket, WriteEntry},
};

impl BucketMap {
	pub fn record_bytes(
		&mut self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		suffix: &[u8],
		post: Option<EncodedPodRow>,
	) {
		self.write_bytes(operator, keyspace, group, suffix, post, false);
	}

	pub fn record_bytes_fresh(
		&mut self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		suffix: &[u8],
		post: Option<EncodedPodRow>,
	) {
		self.write_bytes(operator, keyspace, group, suffix, post, true);
	}

	fn write_bytes(
		&mut self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		suffix: &[u8],
		post: Option<EncodedPodRow>,
		fresh: bool,
	) {
		struct Record<'a> {
			map: &'a mut BucketMap,
			operator: OperatorId,
			group: GroupId,
			suffix: &'a [u8],
			post: Option<EncodedPodRow>,
			fresh: bool,
		}

		impl KeyspaceVisitor for Record<'_> {
			type Output = ();

			fn visit<K: Keyspace>(self) -> Self::Output {
				let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(self.suffix)
					.expect("a stored suffix must decode as its own keyspace's suffix type");
				let bucket = self.map.bucket::<K>(self.operator);
				match self.fresh {
					true => bucket.record_fresh(self.group, suffix, self.post),
					false => bucket.record(self.group, suffix, self.post),
				}
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
				fresh,
			},
		)
		.expect("a write must name a keyspace the catalogue declares");
	}

	pub fn erase_bytes(
		&mut self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		suffix: &[u8],
	) -> bool {
		if !self.buckets.contains_key(&(operator, keyspace)) {
			return false;
		}

		struct Erase<'a> {
			map: &'a mut BucketMap,
			operator: OperatorId,
			group: GroupId,
			suffix: &'a [u8],
		}

		impl KeyspaceVisitor for Erase<'_> {
			type Output = bool;

			fn visit<K: Keyspace>(self) -> Self::Output {
				let Some(suffix) = <K::Suffix as SuffixBytes>::from_suffix_bytes(self.suffix) else {
					return false;
				};
				self.map.bucket::<K>(self.operator).erase(self.group, &suffix)
			}
		}

		dispatch(
			keyspace,
			Erase {
				map: self,
				operator,
				group,
				suffix,
			},
		)
		.unwrap_or(false)
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
					.map(|entry| {
						entry.touch();
						entry.clone()
					})
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
}
