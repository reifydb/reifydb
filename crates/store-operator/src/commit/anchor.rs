// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, ops::Bound};

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::commit::{
	OperatorCommitBuffer,
	batch::{AnchorKey, AnchorSlot, DropMarker},
};

impl OperatorCommitBuffer {
	pub fn record_anchor_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		self.shared
			.inner
			.lock()
			.live
			.anchors
			.insert((operator, group, side, row_number), Some(expiry.to_millis()));
	}

	pub fn record_anchor_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		self.shared.inner.lock().live.anchors.insert((operator, group, side, row_number), None);
	}

	pub fn has_pending_anchor_drop(&self, operator: OperatorId, group: GroupId) -> bool {
		self.shared.inner.lock().any_drop(|marker| match marker {
			DropMarker::OperatorState(candidate) | DropMarker::AnchorsOperator(candidate) => {
				*candidate == operator
			}
			DropMarker::AnchorsGroup(candidate, candidate_group) => {
				*candidate == operator && *candidate_group == group
			}
		})
	}

	pub fn lookup_anchor(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<Option<u64>> {
		let composite = (operator, group, side, row_number);
		let inner = self.shared.inner.lock();
		if let Some(entry) = inner.live.anchors.get(&composite) {
			return Some(*entry);
		}
		inner.in_flight.as_ref().and_then(|batch| batch.anchors.get(&composite).copied())
	}

	pub fn anchors_for_group(&self, operator: OperatorId, group: GroupId) -> Vec<(AnchorSlot, Option<u64>)> {
		let range = (
			Bound::Included((operator, group, u8::MIN, RowNumber(u64::MIN))),
			Bound::Included((operator, group, u8::MAX, RowNumber(u64::MAX))),
		);

		let inner = self.shared.inner.lock();
		let mut merged: BTreeMap<AnchorSlot, Option<u64>> = BTreeMap::new();
		if let Some(batch) = inner.in_flight.as_ref() {
			collect_anchors(&batch.anchors, range, &mut merged);
		}
		collect_anchors(&inner.live.anchors, range, &mut merged);
		merged.into_iter().collect()
	}
}

fn collect_anchors(
	source: &BTreeMap<AnchorKey, Option<u64>>,
	range: (Bound<AnchorKey>, Bound<AnchorKey>),
	out: &mut BTreeMap<AnchorSlot, Option<u64>>,
) {
	for ((_, _, side, row_number), entry) in source.range(range) {
		out.insert((*side, *row_number), *entry);
	}
}
