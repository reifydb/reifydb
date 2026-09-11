// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, KeyspaceId, OperatorStateKey},
};

use crate::resident::bucket::{BucketMap, write::WriteEntry};

impl BucketMap {
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
}

impl IntoIterator for &BucketMap {
	type Item = ((OperatorId, EncodedKey), WriteEntry);
	type IntoIter = std::vec::IntoIter<((OperatorId, EncodedKey), WriteEntry)>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter_encoded().into_iter()
	}
}
