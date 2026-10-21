// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	ops::Bound,
};

use reifydb_codec::{
	key::encoded::EncodedKeyRange,
	row::{operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		any::TaggedKey,
		operator::{
			keyspace::join::{
				JoinExpiryDue, JoinExpiryDueKey, JoinRowExpiry as JoinRowExpirySpace,
				JoinRowExpiryState, JoinRowExpirySuffix, join_expiry_due_key,
			},
			state::{GroupId, GroupStateKey, KeyspaceId, keyspace_inner_range},
		},
		typed::{BoundedKey, direction::Asc},
	},
	state::typed::{SuffixBytes, typed_key},
};
use reifydb_value::{
	Result,
	error::Error as ValueError,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::transaction::{
	FlowTransaction,
	state::{StateExtension, StateRange},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinDueEntry {
	pub at: DateTime,
	pub group: GroupId,
	pub side: u8,
	pub row_number: RowNumber,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DueStart {
	Bottom,
	Floor(DateTime),
	After(JoinExpiryDueKey),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinDuePage {
	pub due: Vec<JoinDueEntry>,
	pub resume: Option<JoinExpiryDueKey>,
	pub more: bool,
	pub next: Option<DateTime>,
}

pub fn join_expiry_key(group: GroupId, side: u8, row_number: RowNumber) -> GroupStateKey {
	typed_key::<JoinRowExpirySpace>(
		group,
		&JoinRowExpirySuffix {
			side: Asc(side),
			row: Asc(row_number),
		},
	)
}

pub fn join_expiry_range(group: GroupId) -> EncodedKeyRange {
	keyspace_inner_range(group, KeyspaceId::JOIN_ROW_EXPIRY)
}

pub fn join_due_floor_key(at: DateTime) -> GroupStateKey {
	typed_key::<JoinExpiryDue>(
		GroupId::ROOT,
		&JoinExpiryDueKey {
			at: Asc(at),
			group: BoundedKey::low(),
			side: BoundedKey::low(),
			row: BoundedKey::low(),
		},
	)
}

pub fn join_due_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, KeyspaceId::JOIN_EXPIRY_DUE)
}

pub trait JoinRowExpiryExtension: FlowTransaction {
	fn join_expiry_clear_many(
		&mut self,
		id: OperatorId,
		side: u8,
		rows: &[(GroupId, RowNumber)],
	) -> Result<Vec<Option<DateTime>>> {
		let keys: Vec<GroupStateKey> =
			rows.iter().map(|&(group, row_number)| join_expiry_key(group, side, row_number)).collect();
		let stored = self.state_get_many(id, &keys)?;
		let mut seen = HashSet::with_capacity(keys.len());
		let mut doomed = Vec::with_capacity(keys.len() * 2);
		let mut cleared = Vec::with_capacity(keys.len());
		for ((&(group, row_number), key), row) in rows.iter().zip(&keys).zip(&stored) {
			let at = match row {
				Some(row) if seen.insert(key.clone()) => {
					JoinRowExpiryState::decode_state(row).map_err(ValueError::from)?.at
				}
				_ => {
					cleared.push(None);
					continue;
				}
			};
			doomed.push(key.clone());
			doomed.push(join_expiry_due_key(at, group, side, row_number));
			cleared.push(Some(at));
		}
		self.state_remove_many(id, &doomed)?;
		Ok(cleared)
	}

	fn join_expiry_arm_many(
		&mut self,
		id: OperatorId,
		side: u8,
		rows: &[(GroupId, RowNumber, DateTime)],
	) -> Result<()> {
		let mut latest: Vec<(GroupId, RowNumber, DateTime)> = Vec::with_capacity(rows.len());
		let mut slot: HashMap<(GroupId, RowNumber), usize> = HashMap::with_capacity(rows.len());
		for &(group, row_number, at) in rows {
			match slot.get(&(group, row_number)) {
				Some(&index) => latest[index].2 = at,
				None => {
					slot.insert((group, row_number), latest.len());
					latest.push((group, row_number, at));
				}
			}
		}
		let keys: Vec<GroupStateKey> =
			latest.iter().map(|&(group, row_number, _)| join_expiry_key(group, side, row_number)).collect();
		let stored = self.state_get_many(id, &keys)?;
		let mut stale = Vec::new();
		let mut armed = Vec::with_capacity(latest.len() * 2);
		for ((&(group, row_number, at), key), row) in latest.iter().zip(keys).zip(&stored) {
			if let Some(row) = row {
				let previous = JoinRowExpiryState::decode_state(row).map_err(ValueError::from)?.at;
				if previous == at {
					continue;
				}
				stale.push(join_expiry_due_key(previous, group, side, row_number));
			}
			armed.push((
				key,
				JoinRowExpiryState {
					at,
				}
				.encode_state()
				.map_err(ValueError::from)?,
			));
			armed.push((join_expiry_due_key(at, group, side, row_number), EncodedPodRow::new(&[])));
		}
		self.state_remove_many(id, &stale)?;
		self.state_set_many(id, armed)
	}

	fn join_expiry_free_many(&mut self, id: OperatorId, entries: &[JoinDueEntry]) -> Result<()> {
		let doomed: Vec<GroupStateKey> = entries
			.iter()
			.flat_map(|entry| {
				[
					join_expiry_key(entry.group, entry.side, entry.row_number),
					join_expiry_due_key(entry.at, entry.group, entry.side, entry.row_number),
				]
			})
			.collect();
		self.state_remove_many(id, &doomed)
	}

	fn join_expiry_min(&mut self, id: OperatorId) -> Result<Option<DateTime>> {
		let batch = self.state_range(
			id,
			StateRange::forward(join_due_range(), "join::expiry_min").limit(1).full_page(),
		)?;
		let Some(row) = batch.items.first() else {
			return Ok(None);
		};
		Ok(decode_due_suffix(&row.key).map(|suffix| suffix.at.0))
	}

	fn join_due_page(
		&mut self,
		id: OperatorId,
		at: DateTime,
		budget: usize,
		start: &DueStart,
	) -> Result<JoinDuePage> {
		if budget == 0 {
			return Ok(JoinDuePage {
				due: Vec::new(),
				resume: None,
				more: false,
				next: None,
			});
		}
		let mut range = join_due_range();
		let site = match start {
			DueStart::After(cursor) => {
				range.start = Bound::Excluded(
					typed_key::<JoinExpiryDue>(GroupId::ROOT, cursor).into_encoded(),
				);
				"join::due_page:resume"
			}
			DueStart::Floor(floor) => {
				range.start = Bound::Included(join_due_floor_key(*floor).into_encoded());
				"join::due_page:floor"
			}
			DueStart::Bottom => "join::due_page:restart",
		};
		let batch = self.state_range(id, StateRange::forward(range, site).limit(budget.saturating_add(1)))?;

		let mut due = Vec::with_capacity(batch.items.len().min(budget));
		let mut more = false;
		let mut resume = None;
		let mut next = None;
		for item in &batch.items {
			let Some(suffix) = decode_due_suffix(&item.key) else {
				continue;
			};
			if suffix.at.0 > at {
				next = Some(suffix.at.0);
				break;
			}
			if due.len() == budget {
				more = true;
				break;
			}
			resume = Some(suffix);
			due.push(JoinDueEntry {
				at: suffix.at.0,
				group: suffix.group.0,
				side: suffix.side.0,
				row_number: suffix.row.0,
			});
		}

		Ok(JoinDuePage {
			due,
			resume,
			more,
			next,
		})
	}
}

impl<T: FlowTransaction> JoinRowExpiryExtension for T {}

fn decode_due_suffix(key: &TaggedKey) -> Option<JoinExpiryDueKey> {
	let TaggedKey::OperatorState(decoded) = key else {
		return None;
	};
	JoinExpiryDueKey::from_suffix_bytes(&decoded.suffix)
}
