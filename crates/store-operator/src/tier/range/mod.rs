// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Range tier of the operator store: a cache of whole buckets, where a bucket is the set of keys sharing one
//! `(operator, group, keyspace)` read at a fixed offset out of the inner key. Operator state is not page
//! addressable, so a key too short to carry both a group and a keyspace is declined rather than cached.
//!
//! Only a scan that covered a bucket's key range end to end may install it, so residency is itself the claim
//! that the bucket holds every key the persistent tier holds in that range. That claim answers a later range
//! over the same key range outright, and answers a point read the flat point cache missed as a definitive
//! absence without storing the absence. Any write that breaks the claim drops the bucket, because there is no
//! weaker state to demote it to.

mod pool;
#[allow(clippy::module_inception)]
mod range;
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
pub struct OperatorRangeConfig {
	pub resident_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl Default for OperatorRangeConfig {
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
	pub const PREFIX_LEN: usize = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize + 1;

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
	entries: BTreeMap<EncodedKey, EncodedPodRow>,
	bytes: usize,
	tick: u64,
}

const NODE_FILL_DIVISOR: usize = 2;

const ENTRY_OVERHEAD: usize = NODE_FILL_DIVISOR * (size_of::<EncodedKey>() + size_of::<EncodedPodRow>());

const BUCKET_OVERHEAD: usize = size_of::<BucketId>() + size_of::<Bucket>();

fn entry_footprint(key: &EncodedKey, row: &EncodedPodRow) -> usize {
	ENTRY_OVERHEAD + key.heap_bytes() + row.len()
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
pub struct OperatorRangeMetrics {
	pub hits: u64,
	pub misses: u64,
	pub fills: u64,
	pub fills_declined: u64,
	pub fills_dirty_aborted: u64,
	pub evictions: u64,
	pub point_hits: u64,
	pub point_misses: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorRangeShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub buckets: usize,
	pub entries: usize,
	pub counters: OperatorRangeMetrics,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorRangeKeyspaceMetrics {
	pub keyspace: Keyspace,
	pub used: ByteSize,
	pub buckets: usize,
	pub entries: usize,
	pub counters: OperatorRangeMetrics,
}

const KEYSPACE_SLOTS: usize = 256;

type KeyspaceCounters = Box<[OperatorRangeMetrics; KEYSPACE_SLOTS]>;

struct RangeFill {
	dirty: bool,
	entries: BTreeMap<EncodedKey, EncodedPodRow>,
	bytes: usize,
}

struct Shard {
	buckets: HashMap<BucketId, Bucket>,
	filling: HashMap<BucketId, RangeFill>,
	budget: MemoryBudget,
	next_tick: u64,
	metrics: OperatorRangeMetrics,
	keyspace_metrics: KeyspaceCounters,
}

#[cfg(test)]
pub(crate) type FillInterlock = Box<dyn Fn(&OperatorRangeTier, BucketId) + Send + Sync>;

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
	#[cfg(test)]
	interlock: Option<FillInterlock>,
}

#[derive(Clone)]
pub struct OperatorRangeTier {
	inner: Arc<PoolInner>,
}
