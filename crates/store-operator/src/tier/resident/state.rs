// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, ops::Bound};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::interface::catalog::flow::OperatorId;

use crate::{
	tier::{
		bucket::{Scan, write::WriteEntry},
		resident::{OperatorResidentState, batch::DropMarker, record_state},
	},
	types::{BufferedState, BufferedStateRange},
};

impl OperatorResidentState {
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

	pub fn state_page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
	) -> BufferedStateRange {
		self.page(operator, start, end, limit, Scan::Forward)
	}

	pub fn state_last_page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
	) -> BufferedStateRange {
		self.page(operator, start, end, limit, Scan::Backward)
	}

	fn page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
		scan: Scan,
	) -> BufferedStateRange {
		let lower = owned(start);
		let upper = owned(end);
		let mut items = Vec::new();

		if let Some(slot) = self.shared().slot(operator) {
			let inner = slot.inner.lock();
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

		BufferedStateRange {
			items,
			dropped: self.shared().dropped(|marker| is_state_drop(marker, operator)),
		}
	}
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
