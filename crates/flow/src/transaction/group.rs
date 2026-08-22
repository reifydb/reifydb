// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::EncodedKey,
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
		operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
	},
	state::group::GroupRecord,
};
use reifydb_value::{Result, reifydb_assertions};

use crate::transaction::{FlowTransaction, state::StateExtension};

fn dictionary_key(keyspace: Keyspace, group: &EncodedKey) -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, keyspace, group)
}

fn record_key(id: GroupId) -> GroupStateKey {
	OperatorStateKey::inner_encoded(id, Keyspace::GROUP_RECORD, vec![])
}

fn counter_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::NODE_COUNTER, vec![])
}

pub(crate) fn encode_payload<T: OperatorState>(value: &T) -> Result<EncodedPodRow> {
	Ok(value.encode_state()?)
}

pub(crate) fn decode_payload<T: OperatorState>(row: &EncodedPodRow) -> Result<T> {
	Ok(decode(row)?)
}

pub(super) fn decode_bytes<T: OperatorState>(bytes: &EncodedBytes) -> Result<T> {
	decode_payload(&EncodedPodRow::from(bytes.clone()))
}

fn stamp(
	txn: &mut impl FlowTransaction,
	operator: OperatorId,
	keyspace: Keyspace,
	id: GroupId,
	group: &EncodedKey,
) -> Result<()> {
	txn.state_set(
		operator,
		&record_key(id),
		encode_payload(&GroupRecord::new(group.as_ref().to_vec(), keyspace.0))?,
	)
}

fn mint(txn: &mut impl FlowTransaction, operator: OperatorId, count: u64) -> Result<u64> {
	let seed = match txn.state_get(operator, &counter_key())? {
		Some(row) => decode_payload::<u64>(&row)?,
		None => GroupId::FIRST.0,
	};
	reifydb_assertions! {
		assert!(
			seed >= GroupId::FIRST.0,
			"group id 0 is reserved for operator scope, where the interning dictionary and the \
			 counter live; minting it would put a real group's state on top of the table that \
			 resolves every group (seed={seed})"
		);
	}
	txn.state_set(operator, &counter_key(), encode_payload(&(seed + count))?)?;
	Ok(seed)
}

pub trait GroupExtension: FlowTransaction {
	fn intern_groups(&mut self, operator: OperatorId, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		self.intern_groups_in(operator, Keyspace::GROUP_DICTIONARY, groups)
	}

	fn intern_groups_in(
		&mut self,
		operator: OperatorId,
		keyspace: Keyspace,
		groups: &[EncodedKey],
	) -> Result<Vec<(GroupId, bool)>> {
		let dictionary_keys: Vec<GroupStateKey> =
			groups.iter().map(|group| dictionary_key(keyspace, group)).collect();

		let batch = self.state_get_many(operator, &dictionary_keys)?;
		let mut found: HashMap<Vec<u8>, EncodedBytes> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_get_many must return OperatorState keys");
			found.insert(decoded.inner().as_slice().to_vec(), item.bytes);
		}

		let mut results: Vec<Option<(GroupId, bool)>> = vec![None; groups.len()];
		let mut new_slots: Vec<bool> = vec![false; dictionary_keys.len()];
		let mut distinct_new: Vec<usize> = Vec::new();
		let mut first_new_slot: HashMap<Vec<u8>, usize> = HashMap::new();
		for (slot, dictionary) in dictionary_keys.iter().enumerate() {
			match found.get(dictionary.as_slice()) {
				Some(existing) => {
					let id = GroupId(decode_bytes::<u64>(existing)?);
					results[slot] = Some((id, false));
				}
				None => {
					new_slots[slot] = true;
					if !first_new_slot.contains_key(dictionary.as_slice()) {
						first_new_slot.insert(dictionary.as_slice().to_vec(), slot);
						distinct_new.push(slot);
					}
				}
			}
		}

		if !distinct_new.is_empty() {
			let start = mint(self, operator, distinct_new.len() as u64)?;
			let mut assigned: HashMap<Vec<u8>, GroupId> = HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let dictionary = &dictionary_keys[slot];
				let id = GroupId(start + offset as u64);
				self.state_set(operator, dictionary, encode_payload(&id.0)?)?;
				stamp(self, operator, keyspace, id, &groups[slot])?;
				assigned.insert(dictionary.as_slice().to_vec(), id);
			}
			for (slot, dictionary) in dictionary_keys.iter().enumerate() {
				if new_slots[slot] {
					let id = assigned[dictionary.as_slice()];
					let is_new = first_new_slot.get(dictionary.as_slice()) == Some(&slot);
					results[slot] = Some((id, is_new));
				}
			}
		}

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	fn lookup_groups(&mut self, operator: OperatorId, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
		self.lookup_groups_in(operator, Keyspace::GROUP_DICTIONARY, groups)
	}

	fn lookup_groups_in(
		&mut self,
		operator: OperatorId,
		keyspace: Keyspace,
		groups: &[EncodedKey],
	) -> Result<Vec<Option<GroupId>>> {
		let dictionary_keys: Vec<GroupStateKey> =
			groups.iter().map(|group| dictionary_key(keyspace, group)).collect();

		let batch = self.state_get_many(operator, &dictionary_keys)?;
		let mut found: HashMap<Vec<u8>, EncodedBytes> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_get_many must return OperatorState keys");
			found.insert(decoded.inner().as_slice().to_vec(), item.bytes);
		}

		dictionary_keys
			.iter()
			.map(|dictionary| match found.get(dictionary.as_slice()) {
				Some(existing) => Ok(Some(GroupId(decode_bytes::<u64>(existing)?))),
				None => Ok(None),
			})
			.collect()
	}

	fn forget_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<bool> {
		self.forget_group_in(operator, Keyspace::GROUP_DICTIONARY, group)
	}

	fn forget_group_in(&mut self, operator: OperatorId, keyspace: Keyspace, group: &EncodedKey) -> Result<bool> {
		let dictionary = dictionary_key(keyspace, group);
		if self.state_get(operator, &dictionary)?.is_none() {
			return Ok(false);
		}
		self.state_remove(operator, &dictionary)?;
		Ok(true)
	}

	fn group_bytes(&mut self, operator: OperatorId, id: GroupId) -> Result<Option<EncodedKey>> {
		Ok(self.group_record(operator, id)?.map(|(bytes, _)| bytes))
	}

	fn stamp_group(
		&mut self,
		operator: OperatorId,
		keyspace: Keyspace,
		id: GroupId,
		group: &EncodedKey,
	) -> Result<()> {
		stamp(self, operator, keyspace, id, group)
	}

	fn group_record(&mut self, operator: OperatorId, id: GroupId) -> Result<Option<(EncodedKey, Keyspace)>> {
		let Some(row) = self.state_get(operator, &record_key(id))? else {
			return Ok(None);
		};
		let record = decode_payload::<GroupRecord>(&row)?;
		Ok(Some((EncodedKey::new(record.group), Keyspace(record.keyspace))))
	}
}

impl<T: FlowTransaction> GroupExtension for T {}
