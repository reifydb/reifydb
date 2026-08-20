// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read buffer tier of the operator store: a single-version cache of point reads that also remembers
//! absences, so a key read many times costs one persistent lookup rather than one per read. Operator state
//! is not page addressable, so entries bucket by `(operator, group, keyspace)` read at a fixed offset out of
//! the inner key; a key too short to carry both a group and a keyspace is declined rather than cached.

mod point;
mod pool;
#[cfg(test)]
mod tests;

use std::{
	collections::{BTreeMap, HashMap},
	mem::size_of,
	sync::Arc,
};

use reifydb_codec::{
	key::{decode_u64, encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

#[derive(Clone, Copy, Debug)]
pub struct OperatorReadBufferConfig {
	pub resident_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl Default for OperatorReadBufferConfig {
	fn default() -> Self {
		Self {
			resident_bytes: Some(ByteSize::from_mib(64)),
			shards: 16,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BucketId {
	pub operator: OperatorId,
	pub group: GroupId,
	pub keyspace: Keyspace,
}

impl BucketId {
	pub fn of(operator: OperatorId, key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		if bytes.len() <= offset {
			return None;
		}
		let group = GroupId(decode_u64(bytes[..offset].try_into().ok()?));
		Some(Self {
			operator,
			group,
			keyspace: Keyspace(encode_u8(bytes[offset])),
		})
	}
}

struct Bucket {
	entries: BTreeMap<EncodedKey, Option<EncodedPodRow>>,
	bytes: usize,
	complete: bool,
	tick: u64,
}

const NODE_FILL_DIVISOR: usize = 2;

const ENTRY_OVERHEAD: usize = NODE_FILL_DIVISOR * (size_of::<EncodedKey>() + size_of::<Option<EncodedPodRow>>());

const BUCKET_OVERHEAD: usize = size_of::<BucketId>() + size_of::<Bucket>();

fn entry_footprint(key: &EncodedKey, row: &Option<EncodedPodRow>) -> usize {
	ENTRY_OVERHEAD + key.heap_bytes() + row.as_ref().map_or(0, EncodedPodRow::len)
}

fn account(bytes: &mut usize, budget: &MemoryBudget, old: usize, new: usize) {
	if new >= old {
		let delta = new - old;
		*bytes += delta;
		budget.charge(ByteSize::from_bytes(delta as u64));
	} else {
		let delta = old - new;
		*bytes -= delta;
		budget.release(ByteSize::from_bytes(delta as u64));
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorReadBufferMetrics {
	pub hits: u64,
	pub misses: u64,
	pub evictions: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorReadBufferShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub buckets: usize,
	pub entries: usize,
	pub complete_buckets: usize,
	pub counters: OperatorReadBufferMetrics,
}

struct Shard {
	buckets: HashMap<BucketId, Bucket>,
	budget: MemoryBudget,
	next_tick: u64,
	metrics: OperatorReadBufferMetrics,
}

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
}

#[derive(Clone)]
pub struct OperatorReadBufferTier {
	inner: Arc<PoolInner>,
}
