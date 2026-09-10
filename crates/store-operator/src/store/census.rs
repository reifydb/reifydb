// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::{KEYSPACES, columns_width},
		state::{KEYSPACE_INNER_PREFIX_LEN, KeyspaceId, OperatorStateKey},
	},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;
use tracing::{instrument, warn};

use crate::{
	error::Result,
	persistent::{Enumerate, PersistentTier},
	store::{OperatorStore, StandardOperatorStore},
	types::{LayeredPre, OperatorStateCensus, OperatorWrite},
};

#[derive(Debug)]
struct Bucket {
	keyspace: KeyspaceId,
	key_width: u64,
	keys: u64,
	value_bytes: u64,
}

impl Bucket {
	fn key_bytes(&self) -> u64 {
		self.keys.saturating_mul(self.key_width)
	}

	fn total_bytes(&self) -> u64 {
		self.key_bytes().saturating_add(self.value_bytes)
	}
}

#[derive(Debug, Default)]
pub(crate) struct OperatorCensus {
	buckets: Mutex<BTreeMap<(OperatorId, KeyspaceId), Bucket>>,
}

impl OperatorCensus {
	pub(crate) fn seeded(persistent: &PersistentTier) -> Self {
		let census = Self::default();
		let entries = match persistent.census() {
			Ok(entries) => entries,
			Err(error) => {
				warn!(error = %error, "operator census failed; seeding an empty census");
				return census;
			}
		};
		let mut buckets = census.buckets.lock();
		for entry in entries {
			if entry.keys == 0 {
				continue;
			}
			let Some(bucket) = slot(&mut buckets, entry.operator, entry.keyspace) else {
				continue;
			};
			bucket.keys = bucket.keys.saturating_add(entry.keys);
			bucket.value_bytes = bucket.value_bytes.saturating_add(entry.value_bytes.as_bytes());
		}
		drop(buckets);
		census
	}

	#[instrument(name = "store::operator::census::record", level = "debug", skip_all, fields(write_count = writes.len()))]
	pub(crate) fn record(&self, writes: &[OperatorWrite]) {
		let mut buckets = self.buckets.lock();
		for write in writes {
			let (operator, key) = match write {
				OperatorWrite::Insert {
					operator,
					key,
					..
				}
				| OperatorWrite::Replace {
					operator,
					key,
					..
				}
				| OperatorWrite::Remove {
					operator,
					key,
					..
				} => (*operator, key),
			};
			let Some((_, keyspace, _)) = OperatorStateKey::decode_inner(key.as_slice()) else {
				continue;
			};
			match write {
				OperatorWrite::Insert {
					post,
					..
				} => {
					let Some(bucket) = slot(&mut buckets, operator, keyspace) else {
						continue;
					};
					bucket.keys = bucket.keys.saturating_add(1);
					bucket.value_bytes =
						bucket.value_bytes.saturating_add(post.bytes().len() as u64);
				}
				OperatorWrite::Replace {
					pre_value_bytes,
					post,
					..
				} => {
					let Some(bucket) = slot(&mut buckets, operator, keyspace) else {
						continue;
					};
					bucket.value_bytes = bucket
						.value_bytes
						.saturating_add(post.bytes().len() as u64)
						.saturating_sub(pre_value_bytes.as_bytes());
				}
				OperatorWrite::Remove {
					pre,
					..
				} => {
					let LayeredPre::Present(pre_value_bytes) = pre else {
						continue;
					};
					let stored = (operator, keyspace);
					let Some(bucket) = buckets.get_mut(&stored) else {
						continue;
					};
					bucket.keys = bucket.keys.saturating_sub(1);
					bucket.value_bytes =
						bucket.value_bytes.saturating_sub(pre_value_bytes.as_bytes());
					if bucket.keys == 0 {
						buckets.remove(&stored);
					}
				}
			}
		}
	}

	pub(crate) fn forget(&self, operator: OperatorId) {
		self.buckets.lock().retain(|(candidate, _), _| *candidate != operator);
	}

	pub(crate) fn snapshot(&self) -> Vec<OperatorStateCensus> {
		self.buckets
			.lock()
			.iter()
			.filter(|(_, bucket)| bucket.keys > 0)
			.map(|((operator, _), bucket)| OperatorStateCensus {
				operator: *operator,
				keyspace: bucket.keyspace,
				keys: bucket.keys,
				key_bytes: ByteSize::from_bytes(bucket.key_bytes()),
				value_bytes: ByteSize::from_bytes(bucket.value_bytes),
			})
			.collect()
	}

	pub(crate) fn bytes(&self, operator: OperatorId) -> ByteSize {
		let total = self
			.buckets
			.lock()
			.iter()
			.filter(|((candidate, _), _)| *candidate == operator)
			.map(|(_, bucket)| bucket.total_bytes())
			.sum();
		ByteSize::from_bytes(total)
	}

	pub(crate) fn total_bytes(&self) -> ByteSize {
		let total = self.buckets.lock().values().map(Bucket::total_bytes).sum();
		ByteSize::from_bytes(total)
	}
}

fn slot(
	buckets: &mut BTreeMap<(OperatorId, KeyspaceId), Bucket>,
	operator: OperatorId,
	keyspace: KeyspaceId,
) -> Option<&mut Bucket> {
	let key_width = key_width(keyspace)?;
	Some(buckets.entry((operator, keyspace)).or_insert(Bucket {
		keyspace,
		key_width,
		keys: 0,
		value_bytes: 0,
	}))
}

fn key_width(keyspace: KeyspaceId) -> Option<u64> {
	KEYSPACES
		.iter()
		.find(|spec| spec.id == keyspace)
		.map(|spec| (KEYSPACE_INNER_PREFIX_LEN + columns_width(spec.suffix)) as u64)
}

impl StandardOperatorStore {
	#[instrument(name = "store::operator::bytes", level = "trace", skip(self), fields(operator = operator.0), ret)]
	pub fn bytes(&self, operator: OperatorId) -> Result<ByteSize> {
		Ok(self.census.bytes(operator))
	}

	#[instrument(name = "store::operator::total_bytes", level = "trace", skip(self), ret)]
	pub fn total_bytes(&self) -> Result<ByteSize> {
		Ok(self.census.total_bytes())
	}

	#[instrument(name = "store::operator::census", level = "debug", skip(self))]
	pub fn census(&self) -> Result<Vec<OperatorStateCensus>> {
		Ok(self.census.snapshot())
	}
}

impl OperatorStore {
	pub fn bytes(&self, operator: OperatorId) -> Result<ByteSize> {
		match self {
			Self::Standard(store) => store.bytes(operator),
		}
	}

	pub fn total_bytes(&self) -> Result<ByteSize> {
		match self {
			Self::Standard(store) => store.total_bytes(),
		}
	}

	pub fn census(&self) -> Result<Vec<OperatorStateCensus>> {
		match self {
			Self::Standard(store) => store.census(),
		}
	}
}
