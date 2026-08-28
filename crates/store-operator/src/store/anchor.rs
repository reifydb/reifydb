// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};
use tracing::instrument;

use crate::{
	store::{OperatorStore, StandardOperatorStore},
	tier::resident::batch::DropMarker,
	types::{BufferedAnchor, OperatorSealAnchor},
};

impl StandardOperatorStore {
	#[instrument(name = "store::operator::anchor_set", level = "debug", skip(self, expiry), fields(operator = operator.0, group = group.0))]
	pub fn anchor_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		self.resident.record_anchor_set(operator, group, side, row_number, expiry);
	}

	#[instrument(name = "store::operator::anchor_remove", level = "debug", skip(self), fields(operator = operator.0, group = group.0))]
	pub fn anchor_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		self.resident.record_anchor_remove(operator, group, side, row_number);
	}

	#[instrument(name = "store::operator::anchors_remove_group", level = "debug", skip(self), fields(operator = operator.0, group = group.0))]
	pub fn anchors_remove_group(&self, operator: OperatorId, group: GroupId) {
		self.resident.record_drop(DropMarker::AnchorsGroup(operator, group));
	}

	#[instrument(name = "store::operator::anchors_drop_operator", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn anchors_drop_operator(&self, operator: OperatorId) {
		self.resident.record_drop(DropMarker::AnchorsOperator(operator));
	}

	#[instrument(name = "store::operator::anchor_get", level = "trace", skip(self), fields(operator = operator.0, group = group.0))]
	pub fn anchor_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		match self.resident.lookup_anchor(operator, group, side, row_number) {
			BufferedAnchor::Expiry(millis) => Some(DateTime::from_millis(millis)),
			BufferedAnchor::Tombstone | BufferedAnchor::Dropped => None,
			BufferedAnchor::Absent => {
				let persistent = self.persistent.as_ref()?;
				if !persistent.anchor_filter().may_contain((operator, group, side, row_number)) {
					return None;
				}
				persistent.anchor_get(operator, group, side, row_number)
			}
		}
	}

	#[instrument(name = "store::operator::anchors_by_expiry", level = "trace", skip(self), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn anchors_by_expiry(&self, operator: OperatorId, group: GroupId, limit: u64) -> Vec<OperatorSealAnchor> {
		self.anchors_scan(operator, group, None, limit)
	}

	#[instrument(name = "store::operator::anchors_due", level = "trace", skip(self, at), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn anchors_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		self.anchors_scan(operator, group, Some(at), limit)
	}

	fn anchors_scan(
		&self,
		operator: OperatorId,
		group: GroupId,
		due: Option<DateTime>,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		let snapshot = self.resident.anchors_for_group(operator, group);
		let buffered = snapshot.anchors;
		let mut merged: Vec<OperatorSealAnchor> = Vec::new();

		if !snapshot.dropped
			&& let Some(persistent) = self.persistent.as_ref()
			&& (snapshot.durable || persistent.anchors_out_of_band())
		{
			let fetch = limit.saturating_add(buffered.len() as u64);
			let rows = match due {
				Some(at) => persistent.anchors_due(operator, group, at, fetch),
				None => persistent.anchors_by_expiry(operator, group, fetch),
			};
			for anchor in rows {
				let slot = (anchor.side, anchor.row_number);
				if buffered.binary_search_by(|(candidate, _)| candidate.cmp(&slot)).is_err() {
					merged.push(anchor);
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
			merged.push(OperatorSealAnchor {
				side,
				row_number,
				expiry: DateTime::from_millis(millis),
			});
		}

		merged.sort_by_key(|anchor| anchor.expiry.to_millis());
		merged.truncate(limit as usize);
		merged
	}
}

impl OperatorStore {
	pub fn anchor_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		match self {
			Self::Standard(store) => store.anchor_set(operator, group, side, row_number, expiry),
		}
	}

	pub fn anchor_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		match self {
			Self::Standard(store) => store.anchor_remove(operator, group, side, row_number),
		}
	}

	pub fn anchors_remove_group(&self, operator: OperatorId, group: GroupId) {
		match self {
			Self::Standard(store) => store.anchors_remove_group(operator, group),
		}
	}

	pub fn anchors_drop_operator(&self, operator: OperatorId) {
		match self {
			Self::Standard(store) => store.anchors_drop_operator(operator),
		}
	}

	pub fn anchor_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		match self {
			Self::Standard(store) => store.anchor_get(operator, group, side, row_number),
		}
	}

	pub fn anchors_by_expiry(&self, operator: OperatorId, group: GroupId, limit: u64) -> Vec<OperatorSealAnchor> {
		match self {
			Self::Standard(store) => store.anchors_by_expiry(operator, group, limit),
		}
	}

	pub fn anchors_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		match self {
			Self::Standard(store) => store.anchors_due(operator, group, at, limit),
		}
	}
}
