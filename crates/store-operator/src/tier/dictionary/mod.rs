// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Dictionary tier of the operator store: the cache that owns the group-interning keyspace on its own. Every
//! dictionary entry of an operator shares one group and one keyspace, so the read buffer collapses the whole
//! dictionary into a single bucket behind a single shard mutex and evicts all of it at once. This tier keys on
//! the suffix instead, which spreads one operator's dictionary across every shard and evicts one entry at a
//! time.
//!
//! The two tiers are mutually exclusive: a key this tier owns never reaches the read buffer, and a key it
//! declines is never cached here.

mod point;
mod pool;
#[cfg(test)]
mod tests;

use std::{collections::HashMap, mem::size_of, sync::Arc};

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
pub struct OperatorDictionaryConfig {
	pub resident_bytes: Option<ByteSize>,
	pub shards: usize,
}

impl Default for OperatorDictionaryConfig {
	fn default() -> Self {
		Self {
			resident_bytes: Some(ByteSize::from_mib(64)),
			shards: 16,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DictionaryKey {
	pub operator: OperatorId,
	pub suffix: Box<[u8]>,
}

impl DictionaryKey {
	pub fn of(operator: OperatorId, key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		if bytes.len() <= offset {
			return None;
		}
		if GroupId(decode_u64(bytes[..offset].try_into().ok()?)) != GroupId::ROOT {
			return None;
		}
		if Keyspace(encode_u8(bytes[offset])) != Keyspace::GROUP_DICTIONARY {
			return None;
		}
		Some(Self {
			operator,
			suffix: bytes[offset + 1..].into(),
		})
	}
}

pub fn owns(key: &EncodedKey) -> bool {
	let bytes = key.as_slice();
	let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
	if bytes.len() <= offset {
		return false;
	}
	let Ok(group) = bytes[..offset].try_into() else {
		return false;
	};
	GroupId(decode_u64(group)) == GroupId::ROOT && Keyspace(encode_u8(bytes[offset])) == Keyspace::GROUP_DICTIONARY
}

struct Slot {
	key: DictionaryKey,
	row: EncodedPodRow,
	tick: u64,
}

const INDEX_FILL_DIVISOR: usize = 2;

const ENTRY_OVERHEAD: usize =
	size_of::<Slot>() + INDEX_FILL_DIVISOR * (size_of::<DictionaryKey>() + size_of::<usize>());

const EVICTION_SAMPLE: usize = 8;

fn entry_footprint(key: &DictionaryKey, row: &EncodedPodRow) -> usize {
	ENTRY_OVERHEAD + INDEX_FILL_DIVISOR * key.suffix.len() + row.len()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorDictionaryMetrics {
	pub hits: u64,
	pub misses: u64,
	pub insertions: u64,
	pub evictions: u64,
	pub fills_started: u64,
	pub fills_dirty_aborted: u64,
	pub fills_duplicate: u64,
}

struct Shard {
	index: HashMap<DictionaryKey, usize>,
	slots: Vec<Slot>,
	budget: MemoryBudget,
	filling: HashMap<DictionaryKey, bool>,
	next_tick: u64,
	rng: u64,
	metrics: OperatorDictionaryMetrics,
}

struct PoolInner {
	shards: Box<[Mutex<Shard>]>,
}

#[derive(Clone)]
pub struct OperatorDictionaryTier {
	inner: Arc<PoolInner>,
}
