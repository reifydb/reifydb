// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, collections::BTreeMap, ops::Bound};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::interface::catalog::flow::OperatorId;

use crate::{
	tier::commit::{
		OperatorCommitBuffer,
		batch::{DropMarker, StateEntry, StateKey},
	},
	types::{BufferedState, BufferedStateRange, DurablePre},
};

impl OperatorCommitBuffer {
	pub fn record_state_set(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
		let mut inner = self.shared.inner.lock();
		inner.live.record_state((operator, key), Some(row), DurablePre::Unknown);
		self.request_flush_when_full(&mut inner);
	}

	pub fn record_state_remove(&self, operator: OperatorId, key: EncodedKey) {
		let mut inner = self.shared.inner.lock();
		inner.live.record_state((operator, key), None, DurablePre::Unknown);
		self.request_flush_when_full(&mut inner);
	}

	pub fn lookup_state(&self, operator: OperatorId, key: &EncodedKey) -> BufferedState {
		let composite = (operator, key.clone());
		let inner = self.shared.inner.lock();
		if let Some(entry) = inner.live.state.get(&composite) {
			return buffered_state(entry);
		}
		if let Some(entry) = inner.in_flight.as_ref().and_then(|batch| batch.state.get(&composite)) {
			return buffered_state(entry);
		}
		if inner.any_drop(|marker| is_state_drop(marker, operator)) {
			return BufferedState::Dropped;
		}
		BufferedState::Absent
	}

	pub fn state_range(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> BufferedStateRange {
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
		let mut merged: BTreeMap<EncodedKey, Option<EncodedPodRow>> = BTreeMap::new();
		if !is_empty_range(&lower, &upper) {
			if let Some(batch) = inner.in_flight.as_ref() {
				collect_state(&batch.state, operator, (lower.clone(), upper.clone()), &mut merged);
			}
			collect_state(&inner.live.state, operator, (lower, upper), &mut merged);
		}
		BufferedStateRange {
			items: merged.into_iter().collect(),
			dropped: inner.any_drop(|marker| is_state_drop(marker, operator)),
		}
	}
}

fn buffered_state(entry: &StateEntry) -> BufferedState {
	match &entry.post {
		Some(row) => BufferedState::Row(row.clone()),
		None => BufferedState::Tombstone,
	}
}

fn is_state_drop(marker: &DropMarker, operator: OperatorId) -> bool {
	match marker {
		DropMarker::OperatorState(candidate) => *candidate == operator,
		DropMarker::AnchorsOperator(_) | DropMarker::AnchorsGroup(_, _) => false,
	}
}

fn is_empty_range(lower: &Bound<StateKey>, upper: &Bound<StateKey>) -> bool {
	let (Bound::Included(start) | Bound::Excluded(start)) = lower else {
		return false;
	};
	let (Bound::Included(end) | Bound::Excluded(end)) = upper else {
		return false;
	};
	match start.cmp(end) {
		Ordering::Greater => true,
		Ordering::Equal => matches!(lower, Bound::Excluded(_)) || matches!(upper, Bound::Excluded(_)),
		Ordering::Less => false,
	}
}

fn collect_state(
	source: &BTreeMap<StateKey, StateEntry>,
	operator: OperatorId,
	range: (Bound<StateKey>, Bound<StateKey>),
	out: &mut BTreeMap<EncodedKey, Option<EncodedPodRow>>,
) {
	for ((candidate, key), entry) in source.range(range) {
		if *candidate != operator {
			break;
		}
		out.insert(key.clone(), entry.post.clone());
	}
}
