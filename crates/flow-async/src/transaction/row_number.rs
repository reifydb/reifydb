// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::state::{OperatorState, decode},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		any::TaggedKey,
		operator::{
			keyspace::{
				join::{JoinRowMapping, JoinRowMappingKey},
				root::{GroupRowMapping, GuestRowMapping, GuestRowMappingSuffix},
			},
			state::{GroupId, GroupStateKey, OperatorStateKey, row_number_counter_key},
		},
		typed::direction::{Asc, Desc},
	},
	state::typed::typed_key,
};
use reifydb_value::{
	Result,
	error::{Error, IntoDiagnostic},
	value::row_number::RowNumber,
};

use crate::{
	error::FlowStateError,
	transaction::{
		FlowTransaction,
		state::{StateExtension, StateRange},
	},
};

const MAPPING_SWEEP_PAGE: usize = 1024;

pub const GUEST_MAPPING_KEY_LEN: usize = 16;

pub fn join_mapping_key(key: &JoinRowMappingKey) -> GroupStateKey {
	typed_key::<JoinRowMapping>(GroupId::ROOT, key)
}

pub fn group_mapping_key(group: GroupId) -> GroupStateKey {
	typed_key::<GroupRowMapping>(group, &())
}

pub fn guest_mapping_key(group: GroupId, key: &EncodedKey) -> Result<GroupStateKey> {
	let mut id = [0u8; GUEST_MAPPING_KEY_LEN];
	let bytes = key.as_slice();
	if bytes.len() > GUEST_MAPPING_KEY_LEN {
		return Err(Error(Box::new(
			FlowStateError::GuestKeyTooWide {
				len: bytes.len(),
			}
			.into_diagnostic(),
		)));
	}
	id[..bytes.len()].copy_from_slice(bytes);
	Ok(typed_key::<GuestRowMapping>(
		group,
		&GuestRowMappingSuffix {
			id: Asc(id),
		},
	))
}

pub fn counter_key() -> GroupStateKey {
	row_number_counter_key()
}

fn mint(txn: &mut impl FlowTransaction, operator: OperatorId, count: u64) -> Result<u64> {
	let mut counter = txn.state_batch(operator, vec![counter_key()])?;
	let seed = match counter.value(0) {
		Some(row) => decode::<u64>(row)?,
		None => 1,
	};
	counter.set(0, (seed + count).encode_state()?);
	txn.state_write_batch(operator, counter)?;
	Ok(seed)
}

fn resolve_or_mint(
	txn: &mut impl FlowTransaction,
	operator: OperatorId,
	map_keys: Vec<GroupStateKey>,
) -> Result<Vec<(RowNumber, bool)>> {
	let mut batch = txn.state_batch(operator, map_keys)?;

	let mut results: Vec<Option<(RowNumber, bool)>> = vec![None; batch.len()];
	let mut new_slots: Vec<bool> = vec![false; batch.len()];
	let mut distinct_new: Vec<usize> = Vec::new();
	let mut first_new_slot: HashMap<GroupStateKey, usize> = HashMap::new();
	for slot in 0..batch.len() {
		match batch.value(slot) {
			Some(existing_row) => {
				results[slot] = Some((RowNumber(decode::<u64>(existing_row)?), false));
			}
			None => {
				new_slots[slot] = true;
				if !first_new_slot.contains_key(batch.key(slot)) {
					first_new_slot.insert(batch.key(slot).clone(), slot);
					distinct_new.push(slot);
				}
			}
		}
	}

	if !distinct_new.is_empty() {
		let start = mint(txn, operator, distinct_new.len() as u64)?;
		let mut assigned: HashMap<GroupStateKey, RowNumber> = HashMap::with_capacity(distinct_new.len());
		for (offset, &slot) in distinct_new.iter().enumerate() {
			let row_number = RowNumber(start + offset as u64);
			batch.set(slot, row_number.0.encode_state()?);
			assigned.insert(batch.key(slot).clone(), row_number);
		}
		for slot in 0..batch.len() {
			if new_slots[slot] {
				let map_key = batch.key(slot);
				results[slot] = Some((assigned[map_key], first_new_slot.get(map_key) == Some(&slot)));
			}
		}
	}

	txn.state_write_batch(operator, batch)?;
	Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
}

pub trait RowNumberExtension: FlowTransaction {
	fn get_or_create_row_numbers(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let map_keys = keys.iter().map(|key| guest_mapping_key(group, key)).collect::<Result<Vec<_>>>()?;
		resolve_or_mint(self, operator, map_keys)
	}

	fn get_or_create_join_row_numbers(
		&mut self,
		operator: OperatorId,
		keys: &[JoinRowMappingKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		resolve_or_mint(self, operator, keys.iter().map(join_mapping_key).collect())
	}

	fn get_or_create_row_numbers_for_groups(
		&mut self,
		operator: OperatorId,
		groups: &[GroupId],
	) -> Result<Vec<(RowNumber, bool)>> {
		resolve_or_mint(self, operator, groups.iter().map(|group| group_mapping_key(*group)).collect())
	}

	fn get_row_numbers_for_groups(
		&mut self,
		operator: OperatorId,
		groups: &[GroupId],
	) -> Result<Vec<Option<RowNumber>>> {
		let map_keys: Vec<GroupStateKey> = groups.iter().map(|group| group_mapping_key(*group)).collect();
		let answers = self.state_get_many(operator, &map_keys)?;

		let mut results: Vec<Option<RowNumber>> = vec![None; groups.len()];
		for (slot, answer) in answers.iter().enumerate() {
			if let Some(existing_row) = answer {
				results[slot] = Some(RowNumber(decode::<u64>(existing_row)?));
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
		let map_keys = keys.iter().map(|key| guest_mapping_key(group, key)).collect::<Result<Vec<_>>>()?;
		self.resolve_row_numbers(operator, map_keys)
	}

	fn get_join_row_numbers(
		&mut self,
		operator: OperatorId,
		keys: &[JoinRowMappingKey],
	) -> Result<Vec<Option<RowNumber>>> {
		self.resolve_row_numbers(operator, keys.iter().map(join_mapping_key).collect())
	}

	fn resolve_row_numbers(
		&mut self,
		operator: OperatorId,
		map_keys: Vec<GroupStateKey>,
	) -> Result<Vec<Option<RowNumber>>> {
		let answers = self.state_get_many(operator, &map_keys)?;

		let mut results: Vec<Option<RowNumber>> = vec![None; map_keys.len()];
		for (slot, answer) in answers.iter().enumerate() {
			if let Some(existing_row) = answer {
				results[slot] = Some(RowNumber(decode::<u64>(existing_row)?));
			}
		}
		Ok(results)
	}

	fn remove_row_number(&mut self, operator: OperatorId, group: GroupId, key: &EncodedKey) -> Result<()> {
		let map_key = guest_mapping_key(group, key)?;
		self.state_remove(operator, &map_key)
	}

	fn remove_row_number_for_group(&mut self, operator: OperatorId, group: GroupId) -> Result<()> {
		self.state_remove(operator, &group_mapping_key(group))
	}

	fn remove_join_row_numbers(&mut self, operator: OperatorId, keys: &[JoinRowMappingKey]) -> Result<()> {
		if keys.is_empty() {
			return Ok(());
		}
		let mut batch = self.state_batch(operator, keys.iter().map(join_mapping_key).collect())?;
		for slot in 0..batch.len() {
			if batch.value(slot).is_some() {
				batch.remove(slot);
			}
		}
		self.state_write_batch(operator, batch)
	}

	fn remove_join_row_numbers_for_left(&mut self, operator: OperatorId, tag: u8, left: u64) -> Result<()> {
		let base = EncodedKeyRange::new(
			Bound::Included(
				join_mapping_key(&JoinRowMappingKey {
					tag: Asc(tag),
					left: Desc(left),
					right: Desc(u64::MAX),
				})
				.into_encoded(),
			),
			Bound::Included(
				join_mapping_key(&JoinRowMappingKey {
					tag: Asc(tag),
					left: Desc(left),
					right: Desc(u64::MIN),
				})
				.into_encoded(),
			),
		);
		let mut lower = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(lower.clone(), base.end.clone());
			let page = self.state_range(
				operator,
				StateRange::forward(range, "rownum::remove_by_prefix").limit(MAPPING_SWEEP_PAGE),
			)?;
			let more = page.has_more;
			let mut doomed = Vec::with_capacity(page.items.len());
			for item in page.items {
				let TaggedKey::OperatorState(decoded) = &item.key else {
					panic!("state_range must return OperatorState keys");
				};
				let inner = OperatorStateKey::inner_encoded(
					decoded.group,
					decoded.keyspace,
					&decoded.suffix,
				);
				lower = Bound::Excluded(inner.as_encoded().clone());
				doomed.push(inner);
			}
			self.state_remove_many(operator, &doomed)?;
			if !more {
				return Ok(());
			}
		}
	}
}

impl<T: FlowTransaction> RowNumberExtension for T {}
