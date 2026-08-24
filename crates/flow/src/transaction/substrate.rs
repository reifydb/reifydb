// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::catalog::flow::FlowId,
	key::operator_state::{Keyspace, OperatorStateKey},
};
use reifydb_store_operator::{
	store::OperatorStore,
	types::{DurablePre, OperatorWrite},
};
use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;

use crate::transaction::{
	anchor::{decode_anchor_expiry, decode_anchor_suffix},
	frontier::OutputFrontiers,
	scope::{OperatorScope, operator_state_coordinates},
};

#[derive(Clone)]
pub struct FlowSubstrate {
	pub dictionary: DictionaryAllocatorRegistry,
	pub frontiers: OutputFrontiers,
	pub operators: Option<OperatorStore>,
}

impl FlowSubstrate {
	pub fn new(dictionary: DictionaryAllocatorRegistry) -> Self {
		Self {
			dictionary,
			frontiers: OutputFrontiers::default(),
			operators: None,
		}
	}

	pub fn with_dictionary(dictionary: DictionaryAllocatorRegistry, operators: OperatorStore) -> Self {
		Self {
			operators: Some(operators),
			..Self::new(dictionary)
		}
	}
}

pub fn apply_operator_state(store: &OperatorStore, pending: &Pending) {
	store.apply_batch(&operator_writes(pending));
}

pub fn apply_operator_state_with_checkpoints(
	store: &OperatorStore,
	pending: &Pending,
	checkpoints: &[(FlowId, CommitVersion)],
	checkpoint_deletes: &[FlowId],
) {
	store.apply_batch_with_checkpoints(&operator_writes(pending), checkpoints, checkpoint_deletes);
}

pub fn operator_writes(pending: &Pending) -> Vec<OperatorWrite> {
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
			(Some((group, (side, row_number))), PendingWrite::Set(row)) => {
				let expiry = decode_anchor_expiry(row)
					.expect("seal anchor rows are written only through SealAnchor");
				match pending.pre_at(key) {
					Some(Some(_)) => OperatorWrite::AnchorReplace {
						operator,
						group,
						side,
						row_num: row_number,
						expiry,
					},
					Some(None) => OperatorWrite::AnchorInsert {
						operator,
						group,
						side,
						row_num: row_number,
						expiry,
					},
					None => panic!(
						"unclassified seal anchor write on operator {}, group {}",
						operator.0, group.0
					),
				}
			}
			(
				Some((group, (side, row_number))),
				PendingWrite::Remove {
					..
				},
			) => OperatorWrite::AnchorRemove {
				operator,
				group,
				side,
				row_num: row_number,
			},
			(None, PendingWrite::Set(row)) => {
				let post = EncodedPodRow::from(row.clone());
				match pending.pre_at(key) {
					Some(Some(pre_value_bytes)) => OperatorWrite::Replace {
						operator,
						key: inner,
						pre_value_bytes,
						post,
					},
					Some(None) => OperatorWrite::Insert {
						operator,
						key: inner,
						post,
					},
					None => panic!("unclassified operator state write on operator {}", operator.0),
				}
			}
			(
				None,
				PendingWrite::Remove {
					..
				},
			) => OperatorWrite::Remove {
				operator,
				key: inner,
				pre: match pending.pre_at(key) {
					Some(Some(bytes)) => DurablePre::Present(bytes),
					Some(None) => DurablePre::Absent,
					None => panic!("unclassified operator state remove on operator {}", operator.0),
				},
			},
		});
	}
	writes
}
