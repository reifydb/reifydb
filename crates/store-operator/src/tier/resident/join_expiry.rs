// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, ops::Bound};

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator::state::GroupId};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::{
	tier::resident::{
		OperatorResidentState,
		batch::{DropMarker, JoinExpiryKey, JoinExpirySlot},
	},
	types::{BufferedJoinExpiry, BufferedJoinExpiryGroup},
};

impl OperatorResidentState {
	pub fn record_join_expiry_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		self.write(|write| {
			write.record_join_expiry((operator, group, side, row_number), Some(expiry.to_millis()), true)
		});
	}

	pub fn record_join_expiry_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		self.write(|write| write.record_join_expiry((operator, group, side, row_number), None, true));
	}

	pub fn lookup_join_expiry(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> BufferedJoinExpiry {
		let composite = (operator, group, side, row_number);
		let inner = self.shared().inner.lock();
		if let Some(entry) = inner.live.join_expiries.get(&composite) {
			return buffered_join_expiry(*entry);
		}
		if let Some(entry) = inner.in_flight.as_ref().and_then(|batch| batch.join_expiries.get(&composite)) {
			return buffered_join_expiry(*entry);
		}
		if inner.any_drop(|marker| is_join_expiry_drop(marker, operator, group)) {
			return BufferedJoinExpiry::Dropped;
		}
		BufferedJoinExpiry::Absent
	}

	pub fn join_expiries_for_group(&self, operator: OperatorId, group: GroupId) -> BufferedJoinExpiryGroup {
		let range = (
			Bound::Included((operator, group, u8::MIN, RowNumber(u64::MIN))),
			Bound::Included((operator, group, u8::MAX, RowNumber(u64::MAX))),
		);

		let inner = self.shared().inner.lock();
		let mut merged: BTreeMap<JoinExpirySlot, Option<u64>> = BTreeMap::new();
		if let Some(batch) = inner.in_flight.as_ref() {
			collect_join_expiries(&batch.join_expiries, range, &mut merged);
		}
		collect_join_expiries(&inner.live.join_expiries, range, &mut merged);
		BufferedJoinExpiryGroup {
			join_expiries: merged.into_iter().collect(),
			dropped: inner.any_drop(|marker| is_join_expiry_drop(marker, operator, group)),
			durable: inner.live.durable_join_expiries.range(range).next().is_some(),
		}
	}
}

fn buffered_join_expiry(entry: Option<u64>) -> BufferedJoinExpiry {
	match entry {
		Some(millis) => BufferedJoinExpiry::Expiry(millis),
		None => BufferedJoinExpiry::Tombstone,
	}
}

fn is_join_expiry_drop(marker: &DropMarker, operator: OperatorId, group: GroupId) -> bool {
	match marker {
		DropMarker::OperatorState(candidate) | DropMarker::JoinExpiriesOperator(candidate) => {
			*candidate == operator
		}
		DropMarker::JoinExpiriesGroup(candidate, candidate_group) => {
			*candidate == operator && *candidate_group == group
		}
	}
}

fn collect_join_expiries(
	source: &BTreeMap<JoinExpiryKey, Option<u64>>,
	range: (Bound<JoinExpiryKey>, Bound<JoinExpiryKey>),
	out: &mut BTreeMap<JoinExpirySlot, Option<u64>>,
) {
	for ((_, _, side, row_number), entry) in source.range(range) {
		out.insert((*side, *row_number), *entry);
	}
}
