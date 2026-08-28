// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	iter::{Peekable, Rev},
	ops::Bound,
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::interface::catalog::flow::OperatorId;

use crate::{
	tier::commit::{
		OperatorCommitBuffer,
		batch::{DropMarker, StateEntry},
		state_map::Range,
	},
	types::{BufferedState, BufferedStateRange, DurablePre},
};

impl OperatorCommitBuffer {
	pub fn record_state_set(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow, pre: DurablePre) {
		self.write(|live| live.record_state((operator, key), Some(row), pre));
	}

	pub fn record_state_remove(&self, operator: OperatorId, key: EncodedKey, pre: DurablePre) {
		self.write(|live| live.record_state((operator, key), None, pre));
	}

	pub fn lookup_state(&self, operator: OperatorId, key: &EncodedKey) -> BufferedState {
		let inner = self.shared().inner.lock();
		if let Some(entry) = inner.live.state.lookup(operator, key) {
			return buffered_state(entry);
		}
		if let Some(entry) = inner.in_flight.as_ref().and_then(|batch| batch.state.lookup(operator, key)) {
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
		self.state_page(operator, start, end, usize::MAX)
	}

	pub fn state_page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
	) -> BufferedStateRange {
		let lower = owned(start);
		let upper = owned(end);

		let inner = self.shared().inner.lock();
		let mut items = Vec::new();
		if limit > 0 && !is_empty_range(&lower, &upper) {
			let live = inner.live.state.range(operator, lower.clone(), upper.clone());
			let flight = match inner.in_flight.as_ref() {
				Some(batch) => batch.state.range(operator, lower, upper),
				None => Range::default(),
			};
			items = merge(live, flight, limit);
		}
		BufferedStateRange {
			items,
			dropped: inner.any_drop(|marker| is_state_drop(marker, operator)),
		}
	}

	pub fn state_last_page(
		&self,
		operator: OperatorId,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
		limit: usize,
	) -> BufferedStateRange {
		let lower = owned(start);
		let upper = owned(end);

		let inner = self.shared().inner.lock();
		let mut items = Vec::new();
		if limit > 0 && !is_empty_range(&lower, &upper) {
			let live = inner.live.state.range(operator, lower.clone(), upper.clone());
			let flight = match inner.in_flight.as_ref() {
				Some(batch) => batch.state.range(operator, lower, upper),
				None => Range::default(),
			};
			items = merge_back(live, flight, limit);
		}
		BufferedStateRange {
			items,
			dropped: inner.any_drop(|marker| is_state_drop(marker, operator)),
		}
	}
}

fn merge(live: Range<'_>, flight: Range<'_>, limit: usize) -> Vec<(EncodedKey, Option<EncodedPodRow>)> {
	let mut live: Peekable<Range<'_>> = live.peekable();
	let mut flight: Peekable<Range<'_>> = flight.peekable();
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

fn merge_back(live: Range<'_>, flight: Range<'_>, limit: usize) -> Vec<(EncodedKey, Option<EncodedPodRow>)> {
	let mut live: Peekable<Rev<Range<'_>>> = live.rev().peekable();
	let mut flight: Peekable<Rev<Range<'_>>> = flight.rev().peekable();
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
