// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::catalog::flow::OperatorId,
	key::operator_state::OperatorStateKey,
};
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;

use crate::transaction::{
	frontier::OutputFrontiers, group::GroupInterner, row_number::RowNumberProvider, timer::TimerWheel,
	watermark::SourceWatermarks,
};

#[derive(Clone, Default)]
pub struct FlowSubstrate {
	pub row: RowNumberProvider,
	pub group: GroupInterner,
	pub dictionary: DictionaryAllocatorRegistry,
	pub watermarks: SourceWatermarks,
	pub frontiers: OutputFrontiers,
	pub timers: TimerWheel,
	pub operators: OperatorStore,
}

impl FlowSubstrate {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_dictionary(dictionary: DictionaryAllocatorRegistry, operators: OperatorStore) -> Self {
		Self {
			dictionary,
			operators,
			..Self::default()
		}
	}
}

pub fn operator_state_coordinates(key: &EncodedKey) -> Option<(OperatorId, EncodedKey)> {
	OperatorStateKey::decode_operator(key)
}

pub fn apply_operator_state(store: &OperatorStore, version: CommitVersion, pending: &Pending) {
	let mut touched: Vec<OperatorId> = Vec::new();
	for (key, write) in pending.iter_sorted() {
		let Some((operator, inner)) = operator_state_coordinates(key) else {
			continue;
		};
		match write {
			PendingWrite::Set(row) => store.set(
				operator,
				inner,
				EncodedOperatorRow::try_from(row.clone())
					.expect("operator state is written only through state_set, which types it"),
			),
			PendingWrite::Remove {
				..
			} => store.remove(operator, &inner),
		}
		if touched.last() != Some(&operator) {
			touched.push(operator);
		}
	}
	if version == CommitVersion(0) {
		return;
	}
}
