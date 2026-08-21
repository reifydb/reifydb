// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read buffer tier of the operator store: a single-version cache of point reads that also remembers
//! absences, so a key read many times costs one persistent lookup rather than one per read. Operator state
//! is not page addressable, so entries bucket by `(operator, group, keyspace)` read at a fixed offset out of
//! the inner key; a key too short to carry both a group and a keyspace is declined rather than cached.
//!
//! A scan that read a whole bucket's key range to the end marks that bucket complete, which lets it answer a
//! later range over its own key range and report a point miss as a definitive absence. Complete buckets are
//! charged to a budget of their own, so a range fill can never buy its rows with the point reads' rows.

mod point;
mod pool;
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
pub struct OperatorReadBufferConfig {
	pub resident_bytes: Option<ByteSize>,
	pub range_resident_bytes: ByteSize,
	pub shards: usize,
}

impl Default for OperatorReadBufferConfig {
	fn default() -> Self {
		Self {
			resident_bytes: Some(ByteSize::from_mib(64)),
			range_resident_bytes: ByteSize::from_mib(32),
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

fn class_budget<'a>(point: &'a MemoryBudget, range: &'a MemoryBudget, complete: bool) -> &'a MemoryBudget {
	if complete {
		range
	} else {
		point
	}
}

fn reclassify(bucket: &mut Bucket, point: &MemoryBudget, range: &MemoryBudget, complete: bool) {
	if bucket.complete == complete {
		return;
	}
	let bytes = ByteSize::from_bytes(bucket.bytes as u64);
	class_budget(point, range, bucket.complete).release(bytes);
	class_budget(point, range, complete).charge(bytes);
	bucket.complete = complete;
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
	pub fills_started: u64,
	pub fills_dirty_aborted: u64,
	pub fills_duplicate: u64,
	pub range_hits: u64,
	pub range_misses: u64,
	pub range_fills: u64,
	pub range_fills_declined: u64,
	pub range_fills_dirty_aborted: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorReadBufferShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub range_used: ByteSize,
	pub range_limit: ByteSize,
	pub buckets: usize,
	pub entries: usize,
	pub complete_buckets: usize,
	pub counters: OperatorReadBufferMetrics,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorReadBufferKeyspaceMetrics {
	pub keyspace: Keyspace,
	pub used: ByteSize,
	pub buckets: usize,
	pub entries: usize,
	pub complete_buckets: usize,
	pub counters: OperatorReadBufferMetrics,
}

const KEYSPACE_SLOTS: usize = 256;

type KeyspaceCounters = Box<[OperatorReadBufferMetrics; KEYSPACE_SLOTS]>;

type FillId = (BucketId, EncodedKey);

struct RangeFill {
	dirty: bool,
	entries: BTreeMap<EncodedKey, Option<EncodedPodRow>>,
	bytes: usize,
}

struct Shard {
	buckets: HashMap<BucketId, Bucket>,
	filling: HashMap<FillId, bool>,
	range_filling: HashMap<BucketId, RangeFill>,
	budget: MemoryBudget,
	range_budget: MemoryBudget,
	next_tick: u64,
	metrics: OperatorReadBufferMetrics,
	keyspace_metrics: KeyspaceCounters,
}

#[cfg(test)]
pub(crate) type FillInterlock = Box<dyn Fn(&OperatorReadBufferTier, BucketId) + Send + Sync>;

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
	#[cfg(test)]
	interlock: Option<FillInterlock>,
}

#[derive(Clone)]
pub struct OperatorReadBufferTier {
	inner: Arc<PoolInner>,
}
