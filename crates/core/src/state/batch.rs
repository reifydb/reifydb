// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_value::{Result, error::Error};

use crate::{
	actors::pending::PendingWrite,
	delta::RemoveVisibility,
	error::diagnostic::flow::flow_state_batch_key_unframed,
	internal_err,
	key::operator::state::{GroupStateKey, is_framed_inner},
};

pub struct StateBatch {
	keys: Vec<GroupStateKey>,
	values: Vec<Option<EncodedPodRow>>,
	ops: Vec<Option<PendingWrite>>,
}

impl StateBatch {
	pub fn read(
		keys: Vec<GroupStateKey>,
		read: impl FnOnce(&[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>>,
	) -> Result<Self> {
		check_batch_keys(&keys)?;
		let values = read(&keys)?;
		if values.len() != keys.len() {
			return internal_err!("a batch read answered {} slots for {} keys", values.len(), keys.len());
		}
		let ops = vec![None; keys.len()];
		Ok(Self {
			keys,
			values,
			ops,
		})
	}

	pub fn from_parts(
		keys: Vec<GroupStateKey>,
		values: Vec<Option<EncodedPodRow>>,
		ops: Vec<Option<PendingWrite>>,
	) -> Result<Self> {
		check_batch_keys(&keys)?;
		if values.len() != keys.len() || ops.len() != keys.len() {
			return internal_err!(
				"a rebuilt batch carries {} keys, {} values and {} ops",
				keys.len(),
				values.len(),
				ops.len()
			);
		}
		Ok(Self {
			keys,
			values,
			ops,
		})
	}

	pub fn len(&self) -> usize {
		self.keys.len()
	}

	pub fn key(&self, slot: usize) -> &GroupStateKey {
		&self.keys[slot]
	}

	pub fn value(&self, slot: usize) -> Option<&EncodedPodRow> {
		self.values[slot].as_ref()
	}

	pub fn op(&self, slot: usize) -> Option<&PendingWrite> {
		self.ops[slot].as_ref()
	}

	pub fn set(&mut self, slot: usize, row: EncodedPodRow) {
		self.ops[slot] = Some(PendingWrite::Set(row.into_bytes()));
	}

	pub fn remove(&mut self, slot: usize) {
		self.ops[slot] = Some(PendingWrite::Remove {
			announce: RemoveVisibility::Silent,
		});
	}

	pub fn into_writes(self) -> Vec<(GroupStateKey, PendingWrite)> {
		let mut folded: Vec<(GroupStateKey, Option<EncodedPodRow>, PendingWrite)> = Vec::new();
		let mut at: HashMap<GroupStateKey, usize> = HashMap::new();
		for ((key, value), op) in self.keys.into_iter().zip(self.values).zip(self.ops) {
			let Some(op) = op else {
				continue;
			};
			match at.get(&key) {
				Some(&index) => folded[index].2 = op,
				None => {
					at.insert(key.clone(), folded.len());
					folded.push((key, value, op));
				}
			}
		}
		folded.into_iter()
			.filter(|(_, value, op)| !unchanged(value.as_ref(), op))
			.map(|(key, _, op)| (key, op))
			.collect()
	}
}

pub fn check_batch_keys(keys: &[GroupStateKey]) -> Result<()> {
	for key in keys {
		check_batch_key(key)?;
	}
	Ok(())
}

pub fn check_batch_key(key: &GroupStateKey) -> Result<()> {
	let inner = key.as_slice();
	if inner.is_empty() || !is_framed_inner(inner) {
		return Err(Error(Box::new(flow_state_batch_key_unframed(inner.len()))));
	}
	Ok(())
}

fn unchanged(value: Option<&EncodedPodRow>, op: &PendingWrite) -> bool {
	match op {
		PendingWrite::Set(bytes) => value.is_some_and(|row| row.bytes() == bytes),
		PendingWrite::Remove {
			..
		} => value.is_none(),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::pod::EncodedPodRow;

	use super::StateBatch;
	use crate::{
		actors::pending::PendingWrite,
		key::operator::{
			keyspace::suffix_width_of,
			state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey},
		},
	};

	fn key(byte: u8) -> GroupStateKey {
		let width = suffix_width_of(KeyspaceId::CUSTOM_UNMANAGED).expect("the fixture keyspace is catalogued");
		OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::CUSTOM_UNMANAGED, vec![byte; width])
	}

	fn set(byte: u8) -> Option<PendingWrite> {
		Some(PendingWrite::Set(EncodedPodRow::new(&[byte]).into_bytes()))
	}

	#[test]
	fn a_rebuilt_batch_refuses_parts_of_unequal_length() {
		// Parts of unequal length pair a key with another slot's read or op, so the host writes the wrong
		// value.
		let rebuilt = StateBatch::from_parts(vec![key(1), key(2)], vec![None, None], vec![set(1), set(2)])
			.expect("parts of equal length rebuild");
		assert_eq!(rebuilt.len(), 2, "precondition: equal parts rebuild every slot");

		let short_values = StateBatch::from_parts(vec![key(1), key(2)], vec![None], vec![set(1), set(2)]);
		let short_ops = StateBatch::from_parts(vec![key(1), key(2)], vec![None, None], vec![set(1)]);
		let long_values =
			StateBatch::from_parts(vec![key(1)], vec![None, Some(EncodedPodRow::new(&[1]))], vec![set(1)]);

		for (name, result) in
			[("short values", short_values), ("short ops", short_ops), ("long values", long_values)]
		{
			let err = result.err().unwrap_or_else(|| panic!("{name} must be refused"));
			assert_eq!(err.0.code, "INTERNAL_ERROR", "{name} must be refused as an internal error");
		}
	}
}
