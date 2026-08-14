// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound, slice::from_ref};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{
		bytes::EncodedBytes,
		operator::{EncodedOperatorRow, OperatorState, decode},
	},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range},
	},
};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::transaction::{FlowTransaction, state::StateExtension};

const ROW_NUMBER_COUNTER_SUFFIX: &[u8] = b"rn";

fn prefix_sweep_probe(operator: OperatorId, found: usize) {
	use std::sync::{Mutex, OnceLock};
	static SEEN: OnceLock<Mutex<(u64, HashMap<u64, (u64, u64)>)>> = OnceLock::new();
	let mut guard = SEEN.get_or_init(|| Mutex::new((0, HashMap::new()))).lock().unwrap();
	guard.0 += 1;
	let entry = guard.1.entry(operator.0).or_insert((0, 0));
	entry.0 += 1;
	entry.1 += found as u64;
	if guard.0 % 20000 == 0 {
		let mut rows: Vec<_> = guard.1.iter().map(|(op, (c, f))| (*op, *c, *f)).collect();
		rows.sort_by_key(|r| std::cmp::Reverse(r.1));
		let total = guard.0;
		println!("PROBE prefix_sweep total_calls={total}");
		for (op, calls, found) in rows.iter().take(20) {
			println!("PROBE   op{op:<5} calls={calls:<8} found={found}");
		}
	}
}

pub fn mapping_key(group: GroupId, key: &EncodedKey) -> GroupStateKey {
	OperatorStateKey::inner_encoded(group, Keyspace::ROW_NUMBER_MAPPING, key)
}

fn mapping_range(group: GroupId) -> EncodedKeyRange {
	keyspace_inner_range(group, Keyspace::ROW_NUMBER_MAPPING)
}

pub fn counter_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::NODE_COUNTER, ROW_NUMBER_COUNTER_SUFFIX)
}

fn decode_bytes<T: OperatorState>(bytes: &EncodedBytes) -> Result<T> {
	Ok(decode(&EncodedOperatorRow::try_from(bytes.clone())?)?)
}

fn mint(txn: &mut impl FlowTransaction, operator: OperatorId, count: u64) -> Result<u64> {
	let seed = match txn.state_get(operator, &counter_key())? {
		Some(row) => decode::<u64>(&row)?,
		None => 1,
	};
	let now = txn.written_at();
	txn.state_set(operator, &counter_key(), (seed + count).encode_state(now)?)?;
	Ok(seed)
}

pub trait RowNumberExtension: FlowTransaction {
	fn get_or_create_row_number(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers(operator, group, from_ref(key))?.into_iter().next().unwrap())
	}

	fn get_or_create_row_numbers(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let now = self.written_at();
		let map_keys: Vec<GroupStateKey> = keys.iter().map(|key| mapping_key(group, key)).collect();

		let batch = self.state_get_many(operator, &map_keys)?;
		let mut found: HashMap<EncodedKey, EncodedBytes> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_get_many must return OperatorState keys");
			found.insert(decoded.inner(), item.bytes);
		}

		let mut results: Vec<Option<(RowNumber, bool)>> = vec![None; keys.len()];
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
			let start = mint(self, operator, distinct_new.len() as u64)?;
			let mut assigned: HashMap<GroupStateKey, RowNumber> =
				HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let map_key = &map_keys[slot];
				let row_number = RowNumber(start + offset as u64);
				self.state_set(operator, map_key, row_number.0.encode_state(now)?)?;
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

	fn get_row_number(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key: &EncodedKey,
	) -> Result<Option<RowNumber>> {
		match self.state_get(operator, &mapping_key(group, key))? {
			Some(existing_row) => Ok(Some(RowNumber(decode::<u64>(&existing_row)?))),
			None => Ok(None),
		}
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

	fn remove_row_number(&mut self, operator: OperatorId, group: GroupId, key: &EncodedKey) -> Result<bool> {
		let map_key = mapping_key(group, key);
		if self.state_get(operator, &map_key)?.is_none() {
			return Ok(false);
		}
		self.state_remove(operator, &map_key)?;
		Ok(true)
	}

	fn remove_row_numbers_below(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		upper: &EncodedKey,
	) -> Result<Vec<RowNumber>> {
		let base = mapping_range(group);
		let boundary = mapping_key(group, upper);
		let range = EncodedKeyRange::new(Bound::Excluded(boundary.into_encoded()), base.end.clone());
		let batch = self.state_range(operator, range, None, "rownum::drop_below")?;

		let mut dropped = Vec::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let inner = OperatorStateKey::inner_encoded(decoded.group, decoded.keyspace, decoded.suffix);
			dropped.push(RowNumber(decode_bytes::<u64>(&item.bytes)?));
			self.state_remove(operator, &inner)?;
		}
		Ok(dropped)
	}

	fn remove_row_numbers_by_prefix(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key_prefix: &[u8],
	) -> Result<()> {
		let inner_prefix = OperatorStateKey::inner_encoded(group, Keyspace::ROW_NUMBER_MAPPING, key_prefix);
		let range = EncodedKeyRange::prefix(inner_prefix.as_ref());
		let batch = self.state_range(operator, range, None, "rownum::remove_by_prefix")?;

		prefix_sweep_probe(operator, batch.items.len());

		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let inner = OperatorStateKey::inner_encoded(decoded.group, decoded.keyspace, decoded.suffix);
			self.state_remove(operator, &inner)?;
		}
		Ok(())
	}
}

impl<T: FlowTransaction> RowNumberExtension for T {}
