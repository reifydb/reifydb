// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator::{
			keyspace::join::{
				JoinExpiryDue, JoinExpiryDueKey, JoinRowExpiry as JoinRowExpirySpace,
				JoinRowExpiryState, JoinRowExpirySuffix, join_expiry_due_key,
			},
			state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey, keyspace_inner_range},
		},
		typed::{TypedKey, direction::Asc},
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
			group: TypedKey::low(),
			side: TypedKey::low(),
			row: TypedKey::low(),
		},
	)
}

pub fn join_due_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, KeyspaceId::JOIN_EXPIRY_DUE)
}

pub fn join_expiry_slot(key: &GroupStateKey) -> Option<JoinRowExpirySuffix> {
	let (_, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_encoded().as_bytes())?;
	(keyspace == KeyspaceId::JOIN_ROW_EXPIRY).then_some(())?;
	JoinRowExpirySuffix::from_suffix_bytes(suffix)
}

pub trait JoinRowExpiryExtension: FlowTransaction {
	fn join_expiry_at(
		&mut self,
		id: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Result<Option<DateTime>> {
		match self.state_get(id, &join_expiry_key(group, side, row_number))? {
			Some(row) => Ok(Some(JoinRowExpiryState::decode_state(&row).map_err(ValueError::from)?.at)),
			None => Ok(None),
		}
	}

	fn join_expiry_arm(
		&mut self,
		id: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		at: DateTime,
	) -> Result<()> {
		if let Some(previous) = self.join_expiry_at(id, group, side, row_number)? {
			if previous == at {
				return Ok(());
			}
			self.state_remove(id, &join_expiry_due_key(previous, group, side, row_number))?;
		}
		self.state_set(
			id,
			&join_expiry_key(group, side, row_number),
			JoinRowExpiryState {
				at,
			}
			.encode_state()
			.map_err(ValueError::from)?,
		)?;
		self.state_set(id, &join_expiry_due_key(at, group, side, row_number), EncodedPodRow::new(&[]))
	}

	fn join_expiry_clear(
		&mut self,
		id: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Result<Option<DateTime>> {
		let Some(at) = self.join_expiry_at(id, group, side, row_number)? else {
			return Ok(None);
		};
		self.join_expiry_free(
			id,
			&JoinDueEntry {
				at,
				group,
				side,
				row_number,
			},
		)?;
		Ok(Some(at))
	}

	fn join_expiry_free(&mut self, id: OperatorId, entry: &JoinDueEntry) -> Result<()> {
		self.state_remove(id, &join_expiry_key(entry.group, entry.side, entry.row_number))?;
		self.state_remove(id, &join_expiry_due_key(entry.at, entry.group, entry.side, entry.row_number))
	}

	fn join_expiry_min(&mut self, id: OperatorId) -> Result<Option<DateTime>> {
		let batch = self.state_range(id, StateRange::forward(join_due_range(), "join::expiry_min").limit(1))?;
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

fn decode_due_suffix(key: &EncodedKey) -> Option<JoinExpiryDueKey> {
	let decoded = OperatorStateKey::decode(key)?;
	JoinExpiryDueKey::from_suffix_bytes(&decoded.suffix)
}
