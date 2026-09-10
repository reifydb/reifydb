// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::catalog::flow::FlowId,
	key::operator::state::GroupStateKey,
};
use reifydb_store_operator::{
	store::OperatorStore,
	types::{LayeredPre, OperatorWrite},
};
use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;
use reifydb_value::{Result, byte_size::ByteSize};

const DEFERRED_SIZING: &str = "deferred write sizing must not fail";

use crate::transaction::{
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
	let deferred = classify_pending(store, pending);
	store.apply_batch(&operator_writes(pending, &deferred));
}

pub fn apply_operator_state_with_checkpoints(
	store: &OperatorStore,
	pending: &Pending,
	checkpoints: &[(FlowId, CommitVersion)],
	checkpoint_deletes: &[FlowId],
) -> Result<()> {
	let deferred = classify_pending(store, pending);
	store.apply_batch_with_checkpoints(&operator_writes(pending, &deferred), checkpoints, checkpoint_deletes)?;
	Ok(())
}

pub type DeferredClassification = HashMap<EncodedKey, Option<ByteSize>>;

pub fn classify_pending(store: &OperatorStore, pending: &Pending) -> DeferredClassification {
	let mut keys = Vec::new();
	let mut probes = Vec::new();
	for (key, _) in pending.iter_sorted() {
		if pending.is_classified(key) {
			continue;
		}
		let Some(OperatorScope {
			operator,
			inner,
		}) = operator_state_coordinates(key)
		else {
			continue;
		};
		keys.push(key.clone());
		probes.push((operator, GroupStateKey::bound_unchecked(inner)));
	}
	if probes.is_empty() {
		return DeferredClassification::new();
	}
	let sizes = store.state_sizes(&probes).expect(DEFERRED_SIZING);
	keys.into_iter().zip(sizes).collect()
}

pub fn operator_writes(pending: &Pending, deferred: &DeferredClassification) -> Vec<OperatorWrite> {
	let mut writes = Vec::with_capacity(pending.len());
	for (key, write) in pending.iter_sorted() {
		let Some(OperatorScope {
			operator,
			inner,
		}) = operator_state_coordinates(key)
		else {
			continue;
		};
		writes.push(match write {
			PendingWrite::Set(row) => {
				let post = EncodedPodRow::from(row.clone());
				match pending.pre_at(key).or_else(|| deferred.get(key).copied()) {
					Some(Some(pre_value_bytes)) => OperatorWrite::Replace {
						operator,
						key: GroupStateKey::bound_unchecked(inner),
						pre_value_bytes,
						post,
					},
					Some(None) => OperatorWrite::Insert {
						operator,
						key: GroupStateKey::bound_unchecked(inner),
						post,
					},
					None => panic!("unclassified operator state write on operator {}", operator.0),
				}
			}
			PendingWrite::Remove {
				..
			} => OperatorWrite::Remove {
				operator,
				key: GroupStateKey::bound_unchecked(inner),
				pre: match pending.pre_at(key).or_else(|| deferred.get(key).copied()) {
					Some(Some(bytes)) => LayeredPre::Present(bytes),
					Some(None) => LayeredPre::Absent,
					None => panic!("unclassified operator state remove on operator {}", operator.0),
				},
			},
		});
	}
	writes
}
