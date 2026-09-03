// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::{KEYSPACES, columns_width},
		state::KeyspaceId,
	},
};
use reifydb_value::byte_size::ByteSize;

use crate::{
	tier::resident::{OperatorResidentState, slot::SlotInner},
	types::OperatorStateCensus,
};

fn key_bytes(keyspace: KeyspaceId) -> u64 {
	let spec = KEYSPACES
		.iter()
		.find(|spec| spec.id == keyspace)
		.expect("an operator state key must name a keyspace in the catalogue");
	columns_width(spec.columns) as u64
}

fn scan_state(inner: &SlotInner, mut visit: impl FnMut(KeyspaceId, &EncodedPodRow)) {
	let operator = inner.live.operator;
	inner.live.state.for_each_entry(operator, |keyspace, _, _, entry| {
		if let Some(row) = &entry.post {
			visit(keyspace, row);
		}
	});
	let Some(pending) = inner.in_flight.as_deref() else {
		return;
	};
	pending.state.for_each_entry(operator, |keyspace, group, suffix, entry| {
		if inner.live.state.get_bytes_ref(operator, keyspace, group, suffix).is_some() {
			return;
		}
		if let Some(row) = &entry.post {
			visit(keyspace, row);
		}
	});
}

impl OperatorResidentState {
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let Some(slot) = self.shared().slot(operator) else {
			return ByteSize::ZERO;
		};
		let inner = slot.inner.lock();
		let mut total = ByteSize::ZERO;
		scan_state(&inner, |keyspace, row| {
			total = total
				.saturating_add(ByteSize::from_bytes(key_bytes(keyspace) + row.bytes().len() as u64));
		});
		total
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
			let mut buckets: BTreeMap<KeyspaceId, OperatorStateCensus> = BTreeMap::new();
			scan_state(&slot.inner.lock(), |keyspace, row| {
				let bucket = buckets.entry(keyspace).or_insert(OperatorStateCensus {
					operator,
					keyspace,
					keys: 0,
					key_bytes: ByteSize::ZERO,
					value_bytes: ByteSize::ZERO,
				});
				bucket.keys += 1;
				bucket.key_bytes =
					bucket.key_bytes.saturating_add(ByteSize::from_bytes(key_bytes(keyspace)));
				bucket.value_bytes = bucket
					.value_bytes
					.saturating_add(ByteSize::from_bytes(row.bytes().len() as u64));
			});
			entries.extend(buckets.into_values());
		}
		entries
	}
}
