// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, ops::Bound};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_runtime::sync::mutex::MutexGuard;
use tracing::instrument;

use crate::{
	resident::{
		Resident,
		bucket::write::WriteEntry,
		record_state,
		slot::{Slot, SlotInner},
	},
	types::{BufferedRange, BufferedState, DropMarker, Scan},
};

impl Resident {
	pub fn record_state_set(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
		self.write_slot(operator, |inner| record_state(inner, key, Some(row)));
	}

	pub fn record_state_remove(&self, operator: OperatorId, key: EncodedKey) {
		self.write_slot(operator, |inner| record_state(inner, key, None));
	}

	pub fn lookup_state(&self, operator: OperatorId, key: &EncodedKey) -> BufferedState {
		if let Some(slot) = self.shared().slot(operator) {
			let found = slot.inner.lock().lookup(key).as_ref().map(buffered_state);
			if let Some(found) = found {
				return found;
			}
		}
		if self.shared().dropped(|marker| is_state_drop(marker, operator)) {
			return BufferedState::Dropped;
		}
		BufferedState::Absent
	}

	pub fn tombstoned<'a>(
		&self,
		operator: OperatorId,
		keys: impl ExactSizeIterator<Item = &'a EncodedKey>,
	) -> Vec<bool> {
		let Some(slot) = self.shared().slot(operator) else {
			return vec![false; keys.len()];
		};
		let inner = lock_slot(&slot, operator);
		keys.map(|key| inner.live.is_deleted(key)).collect()
	}

	pub fn state_page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
	) -> BufferedRange {
		self.page(operator, start, end, limit, Scan::Forward)
	}

	pub fn state_last_page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
	) -> BufferedRange {
		self.page(operator, start, end, limit, Scan::Backward)
	}

	#[instrument(name = "store::operator::resident::state_page", level = "trace", skip(self, start, end), fields(
		operator = operator.0,
		limit = limit
	))]
	fn page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
		scan: Scan,
	) -> BufferedRange {
		let lower = owned(start);
		let upper = owned(end);
		let mut items = Vec::new();

		if let Some(slot) = self.shared().slot(operator) {
			let inner = lock_slot(&slot, operator);
			if limit > 0 && !is_empty_range(&lower, &upper) {
				items = inner
					.live
					.state
					.encoded_range(operator, &lower, &upper, scan, limit)
					.into_iter()
					.map(|(key, entry)| (key, entry.post))
					.collect();
				if scan == Scan::Backward {
					items.reverse();
				}
			}
		}

		BufferedRange {
			items,
			dropped: self.shared().dropped(|marker| is_state_drop(marker, operator)),
		}
	}
}

#[instrument(name = "store::operator::resident::slot_lock", level = "trace", skip(slot), fields(operator = operator.0))]
fn lock_slot(slot: &Slot, operator: OperatorId) -> MutexGuard<'_, SlotInner> {
	slot.inner.lock()
}

fn owned(bound: Bound<&EncodedKey>) -> Bound<EncodedKey> {
	match bound {
		Bound::Included(key) => Bound::Included(key.clone()),
		Bound::Excluded(key) => Bound::Excluded(key.clone()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

fn buffered_state(entry: &WriteEntry) -> BufferedState {
	match &entry.post {
		Some(row) => BufferedState::Row(row.clone()),
		None => BufferedState::Tombstone,
	}
}

fn is_state_drop(marker: &DropMarker, operator: OperatorId) -> bool {
	match marker {
		DropMarker::OperatorState(candidate) => *candidate == operator,
	}
}

fn is_empty_range(lower: &Bound<EncodedKey>, upper: &Bound<EncodedKey>) -> bool {
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
