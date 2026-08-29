// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::byte_size::ByteSize;

use crate::{
	tier::resident::{
		OperatorResidentState,
		slot::{SlotCensus, SlotInner},
	},
	types::{JOIN_EXPIRY_KEY_BYTES, JOIN_EXPIRY_VALUE_BYTES, OperatorStateCensus, StoredJoinRowExpiryCensus},
};

fn scan_state(inner: &SlotInner, mut visit: impl FnMut(&EncodedKey, &EncodedPodRow)) {
	for (key, entry) in inner.live.state.iter() {
		if let Some(row) = &entry.post {
			visit(key, row);
		}
	}
	let Some(pending) = inner.in_flight.as_deref() else {
		return;
	};
	for (key, entry) in pending.state.iter() {
		if inner.live.state.contains_key(key) {
			continue;
		}
		if let Some(row) = &entry.post {
			visit(key, row);
		}
	}
}

fn scan_join_expiries(inner: &SlotInner) -> u64 {
	let mut keys = 0u64;
	for entry in inner.live.join_expiries.values() {
		if entry.is_some() {
			keys += 1;
		}
	}
	let Some(pending) = inner.in_flight.as_deref() else {
		return keys;
	};
	for (key, entry) in pending.join_expiries.iter() {
		if inner.live.join_expiries.contains_key(key) {
			continue;
		}
		if entry.is_some() {
			keys += 1;
		}
	}
	keys
}

fn scanned_census(inner: &SlotInner) -> SlotCensus {
	let mut census = SlotCensus::default();
	scan_state(inner, |key, row| census.admit_state(key, row.bytes().len() as u64));
	census.join_expiries = scan_join_expiries(inner);
	census
}

fn join_expiry_bytes(join_expiries: u64) -> ByteSize {
	(JOIN_EXPIRY_KEY_BYTES + JOIN_EXPIRY_VALUE_BYTES) * join_expiries
}

impl OperatorResidentState {
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let Some(slot) = self.shared().slot(operator) else {
			return ByteSize::ZERO;
		};
		let inner = slot.inner.lock();
		let mut total = ByteSize::ZERO;
		scan_state(&inner, |key, row| {
			total = total.saturating_add(ByteSize::from_bytes(key.len() as u64 + row.bytes().len() as u64));
		});
		total.saturating_add(join_expiry_bytes(scan_join_expiries(&inner)))
	}

	pub fn total_bytes(&self) -> ByteSize {
		let mut total = ByteSize::ZERO;
		for operator in self.shared().operators() {
			total = total.saturating_add(self.bytes(operator));
		}
		total
	}

	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let mut entries = Vec::new();
		for operator in self.shared().operators() {
			let Some(slot) = self.shared().slot(operator) else {
				continue;
			};
			entries.extend(slot.inner.lock().census.entries(operator));
		}
		entries
	}

	pub fn join_expiry_census(&self) -> Vec<StoredJoinRowExpiryCensus> {
		let mut entries = Vec::new();
		for operator in self.shared().operators() {
			let Some(slot) = self.shared().slot(operator) else {
				continue;
			};
			let keys = slot.inner.lock().census.join_expiries;
			if keys == 0 {
				continue;
			}
			entries.push(StoredJoinRowExpiryCensus {
				operator,
				keys,
			});
		}
		entries
	}

	pub fn census_by_scan(&self) -> Vec<OperatorStateCensus> {
		let mut entries = Vec::new();
		for operator in self.shared().operators() {
			let Some(slot) = self.shared().slot(operator) else {
				continue;
			};
			entries.extend(scanned_census(&slot.inner.lock()).entries(operator));
		}
		entries
	}

	pub fn join_expiry_census_by_scan(&self) -> Vec<StoredJoinRowExpiryCensus> {
		let mut entries = Vec::new();
		for operator in self.shared().operators() {
			let Some(slot) = self.shared().slot(operator) else {
				continue;
			};
			let keys = scan_join_expiries(&slot.inner.lock());
			if keys == 0 {
				continue;
			}
			entries.push(StoredJoinRowExpiryCensus {
				operator,
				keys,
			});
		}
		entries
	}
}
