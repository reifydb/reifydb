// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::operator::EncodedOperatorRow;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	key::operator_state::{Keyspace, OperatorStateKey},
};
use reifydb_store_operator::{store::OperatorStore, types::OperatorWrite};
use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;

use crate::{
	timer::wheel::TimerWheel,
	transaction::{
		anchor::{decode_anchor_expiry, decode_anchor_suffix},
		frontier::OutputFrontiers,
		scope::{OperatorScope, operator_state_coordinates},
		watermark::SourceWatermarks,
	},
};

#[derive(Clone, Default)]
pub struct FlowSubstrate {
	pub dictionary: DictionaryAllocatorRegistry,
	pub watermarks: SourceWatermarks,
	pub frontiers: OutputFrontiers,
	pub timers: TimerWheel,
	pub operators: Option<OperatorStore>,
}

impl FlowSubstrate {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_dictionary(dictionary: DictionaryAllocatorRegistry, operators: OperatorStore) -> Self {
		Self {
			dictionary,
			operators: Some(operators),
			..Self::default()
		}
	}
}

pub fn apply_operator_state(store: &OperatorStore, pending: &Pending) {
	let mut writes = Vec::with_capacity(pending.len());
	for (key, write) in pending.iter_sorted() {
		let Some(OperatorScope {
			operator,
			inner,
		}) = operator_state_coordinates(key)
		else {
			continue;
		};
		let anchor = OperatorStateKey::decode_inner(inner.as_slice())
			.filter(|(_, keyspace, _)| *keyspace == Keyspace::SEAL_ANCHOR)
			.map(|(group, _, suffix)| {
				(
					group,
					decode_anchor_suffix(&suffix)
						.expect("seal anchor keys are written only through anchor_key"),
				)
			});
		writes.push(match (anchor, write) {
			(Some((group, (side, row_number))), PendingWrite::Set(row)) => OperatorWrite::AnchorSet {
				operator,
				group,
				side,
				row_number,
				expiry: decode_anchor_expiry(row)
					.expect("seal anchor rows are written only through SealAnchor"),
			},
			(
				Some((group, (side, row_number))),
				PendingWrite::Remove {
					..
				},
			) => OperatorWrite::AnchorRemove {
				operator,
				group,
				side,
				row_number,
			},
			(None, PendingWrite::Set(row)) => OperatorWrite::Set {
				operator,
				key: inner,
				row: EncodedOperatorRow::try_from(row.clone())
					.expect("operator state is written only through state_set, which types it"),
			},
			(
				None,
				PendingWrite::Remove {
					..
				},
			) => OperatorWrite::Remove {
				operator,
				key: inner,
			},
		});
	}
	store.apply_batch(&writes);
}
