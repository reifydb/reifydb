// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Point tier of the operator store: a single-version cache of point reads that also remembers absences, so a
//! key read many times costs one persistent lookup rather than one per read. An entry keys on the whole inner
//! key instead of on a `(operator, group, keyspace)` bucket, so one group's keys spread across every shard and
//! eviction removes one entry rather than every key that happened to share a group.
//!
//! A keyspace the operator store declares uncached is answered from the keyspace byte alone, before any hash
//! or lock, and its miss is charged to a lock free per keyspace counter that folds into the keyspace table.

#[allow(clippy::module_inception)]
mod point;
mod pool;
#[cfg(test)]
mod tests;

use std::{
	collections::HashMap,
	mem::size_of,
	sync::{Arc, atomic::AtomicU64},
};

use reifydb_codec::{
	key::{encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{Keyspace, OperatorStateKey},
	util::budget::MemoryBudget,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::byte_size::ByteSize;

#[derive(Clone, Copy, Debug)]
pub struct OperatorPointConfig {
	pub resident_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl Default for OperatorPointConfig {
	fn default() -> Self {
		Self {
			resident_bytes: Some(ByteSize::from_mib(64)),
			shards: 16,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PointKey {
	pub operator: OperatorId,
	pub key: EncodedKey,
}

fn keyspace_of(key: &EncodedKey) -> Option<Keyspace> {
	let bytes = key.as_slice();
	let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
	if bytes.len() <= offset {
		return None;
	}
	Some(Keyspace(encode_u8(bytes[offset])))
}

struct Slot {
	key: PointKey,
	row: Option<EncodedPodRow>,
	tick: u64,
}

const INDEX_FILL_DIVISOR: usize = 2;

const ENTRY_OVERHEAD: usize = size_of::<Slot>() + INDEX_FILL_DIVISOR * (size_of::<PointKey>() + size_of::<usize>());

const EVICTION_SAMPLE: usize = 8;

const KEYSPACE_SLOTS: usize = 256;

fn entry_footprint(key: &PointKey, row: &Option<EncodedPodRow>) -> usize {
	ENTRY_OVERHEAD + key.key.heap_bytes() + row.as_ref().map_or(0, EncodedPodRow::len)
}

fn account(budget: &MemoryBudget, old: usize, new: usize) {
	if new >= old {
		budget.charge(ByteSize::from_bytes((new - old) as u64));
	} else {
		budget.release(ByteSize::from_bytes((old - new) as u64));
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorPointMetrics {
	pub hits: u64,
	pub misses: u64,
	pub insertions: u64,
	pub evictions: u64,
	pub fills_started: u64,
	pub fills_dirty_aborted: u64,
	pub fills_duplicate: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorPointShardMetrics {
	pub shard: usize,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub entries: usize,
	pub counters: OperatorPointMetrics,
}

#[derive(Clone, Copy, Debug)]
pub struct OperatorPointKeyspaceMetrics {
	pub keyspace: Keyspace,
	pub used: ByteSize,
	pub entries: usize,
	pub counters: OperatorPointMetrics,
}

type KeyspaceCounters = Box<[OperatorPointMetrics; KEYSPACE_SLOTS]>;

struct Shard {
	index: HashMap<PointKey, usize>,
	slots: Vec<Slot>,
	filling: HashMap<PointKey, bool>,
	budget: MemoryBudget,
	next_tick: u64,
	rng: u64,
	metrics: OperatorPointMetrics,
	keyspace_metrics: KeyspaceCounters,
}

#[cfg(test)]
pub(crate) type FillInterlock = Box<dyn Fn(&OperatorPointTier, &PointKey) + Send + Sync>;

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
	excluded_misses: [AtomicU64; KEYSPACE_SLOTS],
	#[cfg(test)]
	interlock: Option<FillInterlock>,
}

#[derive(Clone)]
pub struct OperatorPointTier {
	inner: Arc<PoolInner>,
}
