// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, ops::Bound};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::interface::catalog::flow::OperatorId;

use crate::{
	tier::{
		bucket::write::WriteEntry,
		resident::{OperatorResidentState, batch::DropMarker, record_state},
	},
	types::{BufferedState, BufferedStateRange},
};

type Page = Vec<(EncodedKey, WriteEntry)>;

type CombineFn = fn(&Page, &Page, usize) -> Vec<(EncodedKey, Option<EncodedPodRow>)>;

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

	pub fn state_range(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> BufferedStateRange {
		self.state_page(operator, start, end, usize::MAX)
	}

	pub fn state_page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
	) -> BufferedStateRange {
		self.page(operator, start, end, limit, merge)
	}

	pub fn state_last_page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
	) -> BufferedStateRange {
		self.page(operator, start, end, limit, merge_back)
	}

	fn page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
		combine: CombineFn,
	) -> BufferedStateRange {
		let lower = owned(start);
		let upper = owned(end);
		let mut items = Vec::new();

		if let Some(slot) = self.shared().slot(operator) {
			let inner = slot.inner.lock();
			if limit > 0 && !is_empty_range(&lower, &upper) {
				let live = inner.live.state.encoded_range(operator, &lower, &upper);
				let flight = match inner.in_flight.as_ref() {
					Some(pending) => pending.state.encoded_range(operator, &lower, &upper),
					None => Page::new(),
				};
				items = combine(&live, &flight, limit);
			}
		}

		BufferedStateRange {
			items,
			dropped: self.shared().dropped(|marker| is_state_drop(marker, operator)),
		}
	}
}

fn merge(live: &Page, flight: &Page, limit: usize) -> Vec<(EncodedKey, Option<EncodedPodRow>)> {
	let mut live = live.iter().peekable();
	let mut flight = flight.iter().peekable();
	let mut items = Vec::new();
	while items.len() < limit {
		let winner = match (live.peek(), flight.peek()) {
			(None, None) => break,
			(Some(_), None) => Side::Live,
			(None, Some(_)) => Side::Flight,
			(Some((live_key, _)), Some((flight_key, _))) => match live_key.cmp(flight_key) {
				Ordering::Less => Side::Live,
				Ordering::Greater => Side::Flight,
				Ordering::Equal => Side::Both,
			},
		};
		match winner {
			Side::Live => {
				let (key, entry) = live.next().expect("the peeked live entry is still pending");
				items.push((key.clone(), entry.post.clone()));
			}
			Side::Flight => {
				let (key, entry) = flight.next().expect("the peeked in-flight entry is still pending");
				items.push((key.clone(), entry.post.clone()));
			}
			Side::Both => {
				let (key, entry) = live.next().expect("the peeked live entry is still pending");
				flight.next();
				items.push((key.clone(), entry.post.clone()));
			}
		}
	}
	items
}

fn merge_back(live: &Page, flight: &Page, limit: usize) -> Vec<(EncodedKey, Option<EncodedPodRow>)> {
	let mut live = live.iter().rev().peekable();
	let mut flight = flight.iter().rev().peekable();
	let mut items = Vec::new();
	while items.len() < limit {
		let winner = match (live.peek(), flight.peek()) {
			(None, None) => break,
			(Some(_), None) => Side::Live,
			(None, Some(_)) => Side::Flight,
			(Some((live_key, _)), Some((flight_key, _))) => match live_key.cmp(flight_key) {
				Ordering::Greater => Side::Live,
				Ordering::Less => Side::Flight,
				Ordering::Equal => Side::Both,
			},
		};
		match winner {
			Side::Live => {
				let (key, entry) = live.next().expect("the peeked live entry is still pending");
				items.push((key.clone(), entry.post.clone()));
			}
			Side::Flight => {
				let (key, entry) = flight.next().expect("the peeked in-flight entry is still pending");
				items.push((key.clone(), entry.post.clone()));
			}
			Side::Both => {
				let (key, entry) = live.next().expect("the peeked live entry is still pending");
				flight.next();
				items.push((key.clone(), entry.post.clone()));
			}
		}
	}
	items
}

enum Side {
	Live,
	Flight,
	Both,
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
		DropMarker::JoinExpiriesOperator(_) | DropMarker::JoinExpiriesGroup(_, _) => false,
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
