// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::{collections::HashMap, iter::once, ops::Bound};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_codec::{
	row::operator::EncodedOperatorRow,
	key::{
		encoded::{EncodedKey, EncodedKeyRange},
		serializer::KeySerializer,
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::OperatorId,
	key::{EncodableKey, operator_state::{GroupId, Keyspace, OperatorStateKey}},
};
use reifydb_sdk::flow::operator::state::{decode_payload, encode_payload};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::flow::{
	operator::stateful::utils::{
		internal_state_drop, internal_state_get, internal_state_range_versioned, internal_state_set,
	},
	transaction::FlowTransaction,
};

pub fn allocate_row_numbers(txn: &mut FlowTransaction, node: OperatorId, count: u64) -> Result<u64> {
	let registry = txn.row_allocators();
	let counter_key = counter_key();
	let seed = if registry.is_seeded(node) {
		0
	} else {
		match internal_state_get(node, txn, &counter_key)? {
			Some(row) => decode_payload::<u64>(&row)?,
			None => 1,
		}
	};
	let start = registry.allocate(node, count, seed);
	let high_water = registry.high_water(node).expect("node seeded after allocate");
	let now = txn.clock().now();
	internal_state_set(node, txn, &counter_key, encode_payload(&high_water, now)?)?;
	Ok(start)
}

fn counter_key() -> EncodedKey {
	let mut serializer = KeySerializer::new();
	serializer.extend_u8(Keyspace::NODE_COUNTER.0);
	serializer.finish()
}

const CACHE_CAPACITY: usize = 65_536;

#[derive(Default)]
pub struct RowNumberCache(HashMap<EncodedKey, RowNumber>);

impl RowNumberCache {
	fn remember(&mut self, key: &EncodedKey, row_number: RowNumber) {
		if self.0.len() >= CACHE_CAPACITY {
			self.0.clear();
		}
		self.0.insert(key.clone(), row_number);
	}
}

pub struct RowNumberProvider {
	node: OperatorId,
}

impl RowNumberProvider {
	pub fn new(node: OperatorId) -> Self {
		Self {
			node,
		}
	}

	pub fn get_or_create_row_numbers<'a, I>(
		&self,
		txn: &mut FlowTransaction,
		keys: I,
	) -> Result<Vec<(RowNumber, bool)>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let mut cache = txn.take_cache::<RowNumberCache>(self.node);
		let result = self.get_or_create_row_numbers_with(txn, &mut cache, keys);
		txn.put_cache(self.node, cache);
		result
	}

	fn get_or_create_row_numbers_with<'a, I>(
		&self,
		txn: &mut FlowTransaction,
		cache: &mut RowNumberCache,
		keys: I,
	) -> Result<Vec<(RowNumber, bool)>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let now = txn.clock().now();
		let keys: Vec<&EncodedKey> = keys.into_iter().collect();

		let mut results: Vec<Option<(RowNumber, bool)>> = (0..keys.len()).map(|_| None).collect();
		let mut to_resolve: Vec<usize> = Vec::new();
		for (i, key) in keys.iter().enumerate() {
			match cache.0.get(*key) {
				Some(row_number) => results[i] = Some((*row_number, false)),
				None => to_resolve.push(i),
			}
		}
		if to_resolve.is_empty() {
			return Ok(results.into_iter().map(|r| r.expect("every position filled")).collect());
		}

		let map_keys: Vec<EncodedKey> = to_resolve.iter().map(|i| self.make_map_key(keys[*i])).collect();

		let batch = txn.internal_state_get_many(self.node, &map_keys)?;
		let mut found: HashMap<Vec<u8>, EncodedBytes> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("internal_state_get_many must return operator state keys");
			found.insert(decoded.suffix, item.bytes);
		}

		let mut new_positions: Vec<(usize, EncodedKey)> = Vec::new();

		for (slot, map_key) in map_keys.into_iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(map_key.as_ref()) {
				Some(existing_row) => {
					let existing_row = EncodedOperatorRow::view(existing_row);
					let row_number = RowNumber(decode_payload::<u64>(existing_row)?);
					cache.remember(keys[i], row_number);
					results[i] = Some((row_number, false));
				}
				None => new_positions.push((i, map_key)),
			}
		}

		if !new_positions.is_empty() {
			let start = self.mint(txn, new_positions.len() as u64)?;
			for (offset, (i, map_key)) in new_positions.iter().enumerate() {
				let row_number = RowNumber(start + offset as u64);
				internal_state_set(self.node, txn, map_key, encode_payload(&row_number.0, now)?)?;
				cache.remember(keys[*i], row_number);
				results[*i] = Some((row_number, true));
			}
		}

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	fn mint(&self, txn: &mut FlowTransaction, count: u64) -> Result<u64> {
		allocate_row_numbers(txn, self.node, count)
	}

	pub fn get_or_create_row_number(
		&self,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers(txn, once(key))?.into_iter().next().unwrap())
	}

	pub fn get_row_number(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<RowNumber>> {
		let mut cache = txn.take_cache::<RowNumberCache>(self.node);
		let result = self.get_row_number_with(txn, &mut cache, key);
		txn.put_cache(self.node, cache);
		result
	}

	fn get_row_number_with(
		&self,
		txn: &mut FlowTransaction,
		cache: &mut RowNumberCache,
		key: &EncodedKey,
	) -> Result<Option<RowNumber>> {
		if let Some(row_number) = cache.0.get(key) {
			return Ok(Some(*row_number));
		}
		let map_key = self.make_map_key(key);
		match internal_state_get(self.node, txn, &map_key)? {
			Some(existing_row) => {
				let row_number = RowNumber(decode_payload::<u64>(&existing_row)?);
				cache.remember(key, row_number);
				Ok(Some(row_number))
			}
			None => Ok(None),
		}
	}

	pub fn remove_for_key(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<bool> {
		let mut cache = txn.take_cache::<RowNumberCache>(self.node);
		let cached = cache.0.remove(key).is_some();
		txn.put_cache(self.node, cache);
		let map_key = self.make_map_key(key);
		if !cached && internal_state_get(self.node, txn, &map_key)?.is_none() {
			return Ok(false);
		}
		internal_state_drop(self.node, txn, &map_key)?;
		Ok(true)
	}

	fn make_map_key(&self, key: &EncodedKey) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(Keyspace::ROW_NUMBER_MAPPING.0);
		serializer.extend_bytes(key.as_ref());
		serializer.finish()
	}

	pub fn remove_by_prefix(&self, txn: &mut FlowTransaction, key_prefix: &[u8]) -> Result<()> {
		let mut cache = txn.take_cache::<RowNumberCache>(self.node);
		cache.0.retain(|key, _| !key.as_ref().starts_with(key_prefix));
		txn.put_cache(self.node, cache);

		let mut prefix = Vec::new();
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(Keyspace::ROW_NUMBER_MAPPING.0);
		prefix.extend_from_slice(&serializer.finish());
		prefix.extend_from_slice(key_prefix);

		let state_prefix = OperatorStateKey::new(self.node, GroupId::ROOT, Keyspace::ENGINE_META, prefix.clone());
		let full_range = EncodedKeyRange::prefix(&state_prefix.encode());

		let keys_to_remove = {
			let stream = txn.range(full_range, RangeScope::All, 1024);
			let mut keys = Vec::new();
			for result in stream {
				let multi = result?;
				keys.push(multi.key);
			}
			keys
		};

		for key in keys_to_remove {
			txn.remove(&key)?;
		}

		Ok(())
	}

	pub fn evict_expired(
		&self,
		txn: &mut FlowTransaction,
		cutoff_version: CommitVersion,
		cursor: &mut Option<EncodedKey>,
		batch_size: usize,
	) -> Result<()> {
		let prefix = {
			let mut serializer = KeySerializer::new();
			serializer.extend_u8(Keyspace::ROW_NUMBER_MAPPING.0);
			serializer.finish()
		};
		let base = EncodedKeyRange::prefix(prefix.as_ref());
		let start = match cursor.clone() {
			Some(c) => Bound::Excluded(c),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch = internal_state_range_versioned(self.node, txn, range)
			.take(batch_size)
			.collect::<Result<Vec<_>>>()?;
		let reached_end = batch.len() < batch_size;
		let last_key = batch.last().map(|(key, _, _)| key.clone());

		let mut dropped = false;
		for (key, version, _row) in batch {
			if version > cutoff_version {
				continue;
			}
			internal_state_drop(self.node, txn, &key)?;
			dropped = true;
		}
		if dropped {
			let mut cache = txn.take_cache::<RowNumberCache>(self.node);
			cache.0.clear();
			txn.put_cache(self.node, cache);
		}

		*cursor = if reached_end {
			None
		} else {
			last_key
		};
		Ok(())
	}
}
