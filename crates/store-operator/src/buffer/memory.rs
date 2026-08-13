// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, ops::Bound, sync::Arc};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_runtime::{shutdown::Shutdown, sync::rwlock::RwLock};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::types::{OperatorBatch, OperatorSealAnchor, OperatorSealAnchorCensus, OperatorStateCensus, OperatorWrite};

type StateKey = (OperatorId, EncodedKey);

type AnchorKey = (OperatorId, GroupId, u8, RowNumber);

#[derive(Debug, Default)]
struct Maps {
	state: BTreeMap<StateKey, EncodedOperatorRow>,
	anchors: BTreeMap<AnchorKey, u64>,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryOperatorStorage {
	maps: Arc<RwLock<Maps>>,
}

impl MemoryOperatorStorage {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedOperatorRow) {
		self.maps.write().state.insert((operator, key), row);
	}

	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		self.maps.write().state.remove(&(operator, key.clone()));
	}

	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		let mut maps = self.maps.write();
		for write in writes {
			match write {
				OperatorWrite::Set {
					operator,
					key,
					row,
				} => {
					maps.state.insert((*operator, key.clone()), row.clone());
				}
				OperatorWrite::Remove {
					operator,
					key,
				} => {
					maps.state.remove(&(*operator, key.clone()));
				}
				OperatorWrite::AnchorSet {
					operator,
					group,
					side,
					row_number,
					expiry,
				} => {
					maps.anchors
						.insert((*operator, *group, *side, *row_number), expiry.to_millis());
				}
				OperatorWrite::AnchorRemove {
					operator,
					group,
					side,
					row_number,
				} => {
					maps.anchors.remove(&(*operator, *group, *side, *row_number));
				}
			}
		}
	}

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedOperatorRow> {
		self.maps.read().state.get(&(operator, key.clone())).cloned()
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		self.maps.read().state.contains_key(&(operator, key.clone()))
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		let limit = batch_size.max(1);
		let lower = match range.start.as_ref() {
			Bound::Included(key) => Bound::Included((operator, key.clone())),
			Bound::Excluded(key) => Bound::Excluded((operator, key.clone())),
			Bound::Unbounded => Bound::Included((operator, EncodedKey::new(Vec::new()))),
		};
		let upper = match range.end.as_ref() {
			Bound::Included(key) => Bound::Included((operator, key.clone())),
			Bound::Excluded(key) => Bound::Excluded((operator, key.clone())),
			Bound::Unbounded => Bound::Unbounded,
		};

		let maps = self.maps.read();
		let mut items: Vec<(EncodedKey, EncodedOperatorRow)> = maps
			.state
			.range((lower, upper))
			.take_while(|((candidate, _), _)| *candidate == operator)
			.take(limit as usize + 1)
			.map(|((_, key), row)| (key.clone(), row.clone()))
			.collect();

		let has_more = items.len() as u64 > limit;
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
		}
	}

	pub fn bytes(&self, _operator: OperatorId) -> u64 {
		0
	}

	pub fn total_bytes(&self) -> u64 {
		0
	}

	pub fn census(&self, prefix_len: u32) -> Vec<OperatorStateCensus> {
		let maps = self.maps.read();
		let mut out: Vec<OperatorStateCensus> = Vec::new();
		for ((operator, key), row) in maps.state.iter() {
			let take = (prefix_len as usize).min(key.len());
			let prefix = key.as_slice()[..take].to_vec();
			let key_bytes = key.len() as u64;
			let value_bytes = row.bytes().len() as u64;
			match out.last_mut() {
				Some(last) if last.operator == *operator && last.prefix == prefix => {
					last.keys += 1;
					last.key_bytes += key_bytes;
					last.value_bytes += value_bytes;
				}
				_ => out.push(OperatorStateCensus {
					operator: *operator,
					prefix,
					keys: 1,
					key_bytes,
					value_bytes,
				}),
			}
		}
		out
	}

	pub fn anchor_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		self.maps
			.read()
			.anchors
			.get(&(operator, group, side, row_number))
			.map(|millis| DateTime::from_millis(*millis))
	}

	pub fn anchors_by_expiry(&self, operator: OperatorId, group: GroupId, limit: u64) -> Vec<OperatorSealAnchor> {
		self.scan_anchors(operator, group, limit, |_| true)
	}

	pub fn anchors_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		let due = at.to_millis();
		self.scan_anchors(operator, group, limit, |millis| millis <= due)
	}

	fn scan_anchors(
		&self,
		operator: OperatorId,
		group: GroupId,
		limit: u64,
		keep: impl Fn(u64) -> bool,
	) -> Vec<OperatorSealAnchor> {
		let maps = self.maps.read();
		let mut out: Vec<OperatorSealAnchor> = maps
			.anchors
			.range((
				Bound::Included((operator, group, u8::MIN, RowNumber(u64::MIN))),
				Bound::Included((operator, group, u8::MAX, RowNumber(u64::MAX))),
			))
			.filter(|(_, millis)| keep(**millis))
			.map(|((_, _, side, row_number), millis)| OperatorSealAnchor {
				side: *side,
				row_number: *row_number,
				expiry: DateTime::from_millis(*millis),
			})
			.collect();
		out.sort_by_key(|anchor| anchor.expiry.to_millis());
		out.truncate(limit as usize);
		out
	}

	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		let maps = self.maps.read();
		let mut out: Vec<OperatorSealAnchorCensus> = Vec::new();
		for (operator, group, _, _) in maps.anchors.keys() {
			match out.last_mut() {
				Some(last) if last.operator == *operator && last.group == *group => last.keys += 1,
				_ => out.push(OperatorSealAnchorCensus {
					operator: *operator,
					group: *group,
					keys: 1,
				}),
			}
		}
		out
	}

	pub fn anchor_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		self.maps.write().anchors.insert((operator, group, side, row_number), expiry.to_millis());
	}

	pub fn anchor_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		self.maps.write().anchors.remove(&(operator, group, side, row_number));
	}

	pub fn anchors_remove_group(&self, operator: OperatorId, group: GroupId) {
		self.maps.write().anchors.retain(|(candidate, candidate_group, _, _), _| {
			*candidate != operator || *candidate_group != group
		});
	}

	pub fn anchors_drop_operator(&self, operator: OperatorId) {
		self.maps.write().anchors.retain(|(candidate, _, _, _), _| *candidate != operator);
	}

	pub fn drop_operator_state(&self, operator: OperatorId) {
		let mut maps = self.maps.write();
		maps.state.retain(|(candidate, _), _| *candidate != operator);
		maps.anchors.retain(|(candidate, _, _, _), _| *candidate != operator);
	}
}

impl Shutdown for MemoryOperatorStorage {
	fn shutdown(&self) {
		let mut maps = self.maps.write();
		maps.state.clear();
		maps.anchors.clear();
	}
}
