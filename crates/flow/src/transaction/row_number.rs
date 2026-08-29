// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	ops::Bound,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{
		bytes::EncodedBytes,
		operator::state::{OperatorState, decode},
		pod::EncodedPodRow,
	},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{
			GroupId, GroupStateKey, KeyspaceId, OperatorStateKey,
			row_number_counter_key,
		},
	},
};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::transaction::{
	FlowTransaction,
	state::{StateExtension, StateRange},
};

const MAPPING_SWEEP_PAGE: usize = 1024;

pub fn mapping_key(group: GroupId, key: &EncodedKey) -> GroupStateKey {
	OperatorStateKey::inner_encoded(group, KeyspaceId::ROW_NUMBER_MAPPING, key)
}

pub fn counter_key() -> GroupStateKey {
	row_number_counter_key()
}

fn present_keys(
	txn: &mut impl FlowTransaction,
	operator: OperatorId,
	map_keys: &[GroupStateKey],
) -> Result<HashSet<EncodedKey>> {
	let batch = txn.state_get_many(operator, map_keys)?;
	let mut present = HashSet::with_capacity(batch.items.len());
	for item in batch.items {
		let decoded =
			OperatorStateKey::decode(&item.key).expect("state_get_many must return OperatorState keys");
		present.insert(decoded.inner());
	}
	Ok(present)
}

fn decode_bytes<T: OperatorState>(bytes: &EncodedBytes) -> Result<T> {
	Ok(decode(&EncodedPodRow::from(bytes.clone()))?)
}

fn mint(txn: &mut impl FlowTransaction, operator: OperatorId, count: u64) -> Result<u64> {
	let seed = match txn.state_get(operator, &counter_key())? {
		Some(row) => decode::<u64>(&row)?,
		None => 1,
	};
	txn.state_set(operator, &counter_key(), (seed + count).encode_state()?)?;
	Ok(seed)
}

fn resolve_or_mint(
	txn: &mut impl FlowTransaction,
	operator: OperatorId,
	map_keys: Vec<GroupStateKey>,
) -> Result<Vec<(RowNumber, bool)>> {
	let batch = txn.state_get_many(operator, &map_keys)?;
	let mut found: HashMap<EncodedKey, EncodedBytes> = HashMap::with_capacity(batch.items.len());
	for item in batch.items {
		let decoded =
			OperatorStateKey::decode(&item.key).expect("state_get_many must return OperatorState keys");
		found.insert(decoded.inner(), item.bytes);
	}

	let mut results: Vec<Option<(RowNumber, bool)>> = vec![None; map_keys.len()];
	let mut new_slots: Vec<bool> = vec![false; map_keys.len()];
	let mut distinct_new: Vec<usize> = Vec::new();
	let mut first_new_slot: HashMap<GroupStateKey, usize> = HashMap::new();
	for (slot, map_key) in map_keys.iter().enumerate() {
		match found.get(map_key.as_slice()) {
			Some(existing_row) => {
				results[slot] = Some((RowNumber(decode_bytes::<u64>(existing_row)?), false));
			}
			None => {
				new_slots[slot] = true;
				if !first_new_slot.contains_key(map_key) {
					first_new_slot.insert(map_key.clone(), slot);
					distinct_new.push(slot);
				}
			}
		}
	}

	if !distinct_new.is_empty() {
		let start = mint(txn, operator, distinct_new.len() as u64)?;
		let mut assigned: HashMap<GroupStateKey, RowNumber> = HashMap::with_capacity(distinct_new.len());
		for (offset, &slot) in distinct_new.iter().enumerate() {
			let map_key = &map_keys[slot];
			let row_number = RowNumber(start + offset as u64);
			txn.state_set(operator, map_key, row_number.0.encode_state()?)?;
			assigned.insert(map_key.clone(), row_number);
		}
		for (slot, map_key) in map_keys.iter().enumerate() {
			if new_slots[slot] {
				let row_number = assigned[map_key];
				let is_new = first_new_slot.get(map_key) == Some(&slot);
				results[slot] = Some((row_number, is_new));
			}
		}
	}

	Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
}

pub trait RowNumberExtension: FlowTransaction {
	fn get_or_create_row_numbers(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		resolve_or_mint(self, operator, keys.iter().map(|key| mapping_key(group, key)).collect())
	}

	fn get_or_create_row_numbers_for_groups(
		&mut self,
		operator: OperatorId,
		groups: &[GroupId],
		key: &EncodedKey,
	) -> Result<Vec<(RowNumber, bool)>> {
		resolve_or_mint(self, operator, groups.iter().map(|group| mapping_key(*group, key)).collect())
	}

	fn get_or_create_row_numbers_for_pairs(
		&mut self,
		operator: OperatorId,
		pairs: &[(GroupId, EncodedKey)],
	) -> Result<Vec<(RowNumber, bool)>> {
		resolve_or_mint(self, operator, pairs.iter().map(|(group, key)| mapping_key(*group, key)).collect())
	}

	fn get_row_numbers_for_groups(
		&mut self,
		operator: OperatorId,
		groups: &[GroupId],
		key: &EncodedKey,
	) -> Result<Vec<Option<RowNumber>>> {
		let map_keys: Vec<GroupStateKey> = groups.iter().map(|group| mapping_key(*group, key)).collect();
		let batch = self.state_get_many(operator, &map_keys)?;
		let mut found: HashMap<EncodedKey, EncodedBytes> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_get_many must return OperatorState keys");
			found.insert(decoded.inner(), item.bytes);
		}

		let mut results: Vec<Option<RowNumber>> = vec![None; groups.len()];
		for (slot, map_key) in map_keys.iter().enumerate() {
			if let Some(existing_row) = found.get(map_key.as_slice()) {
				results[slot] = Some(RowNumber(decode_bytes::<u64>(existing_row)?));
			}
		}
		Ok(results)
	}

	fn get_row_numbers(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>> {
		let map_keys: Vec<GroupStateKey> = keys.iter().map(|key| mapping_key(group, key)).collect();
		let batch = self.state_get_many(operator, &map_keys)?;
		let mut found: HashMap<EncodedKey, EncodedBytes> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_get_many must return OperatorState keys");
			found.insert(decoded.inner(), item.bytes);
		}

		let mut results: Vec<Option<RowNumber>> = vec![None; keys.len()];
		for (slot, map_key) in map_keys.iter().enumerate() {
			if let Some(existing_row) = found.get(map_key.as_slice()) {
				results[slot] = Some(RowNumber(decode_bytes::<u64>(existing_row)?));
			}
		}
		Ok(results)
	}

	fn remove_row_number(&mut self, operator: OperatorId, group: GroupId, key: &EncodedKey) -> Result<()> {
		self.state_remove(operator, &mapping_key(group, key))
	}

	fn remove_row_numbers(&mut self, operator: OperatorId, group: GroupId, keys: &[EncodedKey]) -> Result<()> {
		if keys.is_empty() {
			return Ok(());
		}
		let map_keys: Vec<GroupStateKey> = keys.iter().map(|key| mapping_key(group, key)).collect();
		let present = present_keys(self, operator, &map_keys)?;
		for map_key in map_keys {
			if present.contains(map_key.as_slice()) {
				self.state_remove(operator, &map_key)?;
			}
		}
		Ok(())
	}

	fn remove_row_numbers_by_prefix(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key_prefix: &[u8],
	) -> Result<()> {
		let inner_prefix = OperatorStateKey::inner_encoded(group, KeyspaceId::ROW_NUMBER_MAPPING, key_prefix);
		let base = EncodedKeyRange::prefix(inner_prefix.as_ref());
		let mut lower = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(lower.clone(), base.end.clone());
			let batch = self.state_range(
				operator,
				StateRange::forward(range, "rownum::remove_by_prefix").limit(MAPPING_SWEEP_PAGE),
			)?;
			let more = batch.has_more;
			for item in batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				let inner = OperatorStateKey::inner_encoded(
					decoded.group,
					decoded.keyspace,
					decoded.suffix,
				);
				lower = Bound::Excluded(inner.as_encoded().clone());
				self.state_remove(operator, &inner)?;
			}
			if !more {
				return Ok(());
			}
		}
	}
}

impl<T: FlowTransaction> RowNumberExtension for T {}
