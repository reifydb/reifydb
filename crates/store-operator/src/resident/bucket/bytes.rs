// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
	Bucket, BucketMap,
	write::{StandardBucket, WriteEntry},
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

	pub fn get_bytes_ref(
		&self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		group: GroupId,
		suffix: &[u8],
	) -> Option<WriteEntry> {
		struct Get<'a> {
			bucket: &'a dyn Bucket,
			group: GroupId,
			suffix: &'a [u8],
		}

		impl KeyspaceVisitor for Get<'_> {
			type Output = Option<WriteEntry>;

			fn visit<K: Keyspace>(self) -> Self::Output {
				let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(self.suffix)?;
				self.bucket
					.as_any()
					.downcast_ref::<StandardBucket<K>>()
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
