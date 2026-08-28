// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};
use tracing::instrument;

use crate::{
	store::{OperatorStore, StandardOperatorStore},
	tier::resident::batch::DropMarker,
	types::{BufferedJoinExpiry, StoredJoinRowExpiry},
};

impl StandardOperatorStore {
	#[instrument(name = "store::operator::join_expiry_set", level = "debug", skip(self, expiry), fields(operator = operator.0, group = group.0))]
	pub fn join_expiry_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		self.resident.record_join_expiry_set(operator, group, side, row_number, expiry);
	}

	#[instrument(name = "store::operator::join_expiry_remove", level = "debug", skip(self), fields(operator = operator.0, group = group.0))]
	pub fn join_expiry_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		self.resident.record_join_expiry_remove(operator, group, side, row_number);
	}

	#[instrument(name = "store::operator::join_expiries_remove_group", level = "debug", skip(self), fields(operator = operator.0, group = group.0))]
	pub fn join_expiries_remove_group(&self, operator: OperatorId, group: GroupId) {
		self.resident.record_drop(DropMarker::JoinExpiriesGroup(operator, group));
	}

	#[instrument(name = "store::operator::join_expiries_drop_operator", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn join_expiries_drop_operator(&self, operator: OperatorId) {
		self.resident.record_drop(DropMarker::JoinExpiriesOperator(operator));
	}

	#[instrument(name = "store::operator::join_expiry_get", level = "trace", skip(self), fields(operator = operator.0, group = group.0))]
	pub fn join_expiry_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		match self.resident.lookup_join_expiry(operator, group, side, row_number) {
			BufferedJoinExpiry::Expiry(millis) => Some(DateTime::from_millis(millis)),
			BufferedJoinExpiry::Tombstone | BufferedJoinExpiry::Dropped => None,
			BufferedJoinExpiry::Absent => {
				let persistent = self.persistent.as_ref()?;
				if !persistent.join_expiry_filter().may_contain((operator, group, side, row_number)) {
					return None;
				}
				persistent.join_expiry_get(operator, group, side, row_number)
			}
		}
	}

	#[instrument(name = "store::operator::join_expiries_by_time", level = "trace", skip(self), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn join_expiries_by_time(
		&self,
		operator: OperatorId,
		group: GroupId,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		self.join_expiries_scan(operator, group, None, limit)
	}

	#[instrument(name = "store::operator::join_expiries_due", level = "trace", skip(self, at), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn join_expiries_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		self.join_expiries_scan(operator, group, Some(at), limit)
	}

	fn join_expiries_scan(
		&self,
		operator: OperatorId,
		group: GroupId,
		due: Option<DateTime>,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		let snapshot = self.resident.join_expiries_for_group(operator, group);
		let buffered = snapshot.join_expiries;
		let mut merged: Vec<StoredJoinRowExpiry> = Vec::new();

		if !snapshot.dropped
			&& let Some(persistent) = self.persistent.as_ref()
			&& (snapshot.durable || persistent.join_expiries_out_of_band())
		{
			let fetch = limit.saturating_add(buffered.len() as u64);
			let rows = match due {
				Some(at) => persistent.join_expiries_due(operator, group, at, fetch),
				None => persistent.join_expiries_by_time(operator, group, fetch),
			};
			for join_expiry in rows {
				let slot = (join_expiry.side, join_expiry.row_number);
				if buffered.binary_search_by(|(candidate, _)| candidate.cmp(&slot)).is_err() {
					merged.push(join_expiry);
				}
			}
		}

		for ((side, row_number), entry) in buffered {
			let Some(millis) = entry else {
				continue;
			};
			if due.is_some_and(|at| millis > at.to_millis()) {
				continue;
			}
			merged.push(StoredJoinRowExpiry {
				side,
				row_number,
				at: DateTime::from_millis(millis),
			});
		}

		merged.sort_by_key(|join_expiry| join_expiry.at.to_millis());
		merged.truncate(limit as usize);
		merged
	}
}

impl OperatorStore {
	pub fn join_expiry_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		match self {
			Self::Standard(store) => store.join_expiry_set(operator, group, side, row_number, expiry),
		}
	}

	pub fn join_expiry_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		match self {
			Self::Standard(store) => store.join_expiry_remove(operator, group, side, row_number),
		}
	}

	pub fn join_expiries_remove_group(&self, operator: OperatorId, group: GroupId) {
		match self {
			Self::Standard(store) => store.join_expiries_remove_group(operator, group),
		}
	}

	pub fn join_expiries_drop_operator(&self, operator: OperatorId) {
		match self {
			Self::Standard(store) => store.join_expiries_drop_operator(operator),
		}
	}

	pub fn join_expiry_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		match self {
			Self::Standard(store) => store.join_expiry_get(operator, group, side, row_number),
		}
	}

	pub fn join_expiries_by_time(
		&self,
		operator: OperatorId,
		group: GroupId,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		match self {
			Self::Standard(store) => store.join_expiries_by_time(operator, group, limit),
		}
	}

	pub fn join_expiries_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		match self {
			Self::Standard(store) => store.join_expiries_due(operator, group, at, limit),
		}
	}
}
