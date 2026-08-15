// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, ops::Bound};

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::interface::catalog::flow::OperatorId;

use crate::commit::{
	OperatorCommitBuffer,
	batch::{DropMarker, StateKey},
};

impl OperatorCommitBuffer {
	pub fn record_state_set(&self, operator: OperatorId, key: EncodedKey, row: EncodedOperatorRow) {
		self.shared.inner.lock().live.state.insert((operator, key), Some(row));
	}

	pub fn record_state_remove(&self, operator: OperatorId, key: EncodedKey) {
		self.shared.inner.lock().live.state.insert((operator, key), None);
	}

	pub fn has_pending_state_drop(&self, operator: OperatorId) -> bool {
		self.shared.inner.lock().any_drop(|marker| match marker {
			DropMarker::OperatorState(candidate) => *candidate == operator,
			DropMarker::AnchorsOperator(_) | DropMarker::AnchorsGroup(_, _) => false,
		})
	}

	pub fn lookup_state(&self, operator: OperatorId, key: &EncodedKey) -> Option<Option<EncodedOperatorRow>> {
		let composite = (operator, key.clone());
		let inner = self.shared.inner.lock();
		if let Some(entry) = inner.live.state.get(&composite) {
			return Some(entry.clone());
		}
		inner.in_flight.as_ref().and_then(|batch| batch.state.get(&composite).cloned())
	}

	pub fn state_range(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Vec<(EncodedKey, Option<EncodedOperatorRow>)> {
		let lower = match start {
			Bound::Included(key) => Bound::Included((operator, key.clone())),
			Bound::Excluded(key) => Bound::Excluded((operator, key.clone())),
			Bound::Unbounded => Bound::Included((operator, EncodedKey::new(Vec::new()))),
		};
		let upper = match end {
			Bound::Included(key) => Bound::Included((operator, key.clone())),
			Bound::Excluded(key) => Bound::Excluded((operator, key.clone())),
			Bound::Unbounded => Bound::Unbounded,
		};

		let inner = self.shared.inner.lock();
		let mut merged: BTreeMap<EncodedKey, Option<EncodedOperatorRow>> = BTreeMap::new();
		if let Some(batch) = inner.in_flight.as_ref() {
			collect_state(&batch.state, operator, (lower.clone(), upper.clone()), &mut merged);
		}
		collect_state(&inner.live.state, operator, (lower, upper), &mut merged);
		merged.into_iter().collect()
	}
}

fn collect_state(
	source: &BTreeMap<StateKey, Option<EncodedOperatorRow>>,
	operator: OperatorId,
	range: (Bound<StateKey>, Bound<StateKey>),
	out: &mut BTreeMap<EncodedKey, Option<EncodedOperatorRow>>,
) {
	for ((candidate, key), entry) in source.range(range) {
		if *candidate != operator {
			break;
		}
		out.insert(key.clone(), entry.clone());
	}
}
