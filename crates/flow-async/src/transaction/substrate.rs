// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::flow::FlowId,
		operator::{OperatorSlice, OperatorStateWrite},
	},
	key::operator::state::GroupStateKey,
};
use reifydb_store_operator::store::OperatorStore;
#[cfg(test)]
use reifydb_store_operator::types::classify_slice;
use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;
use reifydb_value::{Result, byte_size::ByteSize};
#[cfg(test)]
use reifydb_wal::lsn::Lsn;
use tracing::instrument;

use crate::{
	error::FlowStateError,
	transaction::{
		frontier::OutputFrontiers,
		scope::{OperatorScope, operator_state_coordinates},
	},
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

#[instrument(name = "flow::substrate::operator_section", level = "trace", skip_all, fields(
	pending_count = pending.len(),
	checkpoint_count = checkpoints.len()
))]
pub fn operator_section(
	store: &OperatorStore,
	pending: &Pending,
	checkpoints: &[(FlowId, CommitVersion)],
	deletes: &[FlowId],
) -> Result<(OperatorSlice, Vec<Option<ByteSize>>)> {
	let mut writes = Vec::new();
	let mut sizes = Vec::new();
	let mut unsized_at = Vec::new();
	let mut probes = Vec::new();
	for (key, write) in pending.state_sorted() {
		let Some(OperatorScope {
			operator,
			inner,
		}) = operator_state_coordinates(key)
		else {
			return Err(FlowStateError::PendingStateKeyUndecodable {
				key: key.clone(),
			}
			.into());
		};
		let state_key = GroupStateKey::bound_unchecked(inner);
		match pending.pre_at(key) {
			Some(size) => sizes.push(size),
			None => {
				unsized_at.push(sizes.len());
				probes.push((operator, state_key.clone()));
				sizes.push(None);
			}
		}
		writes.push((
			operator,
			state_key,
			match write {
				PendingWrite::Set(row) => OperatorStateWrite::Set(EncodedPodRow::from(row.clone())),
				PendingWrite::Remove {
					..
				} => OperatorStateWrite::Remove,
			},
		));
	}
	if !probes.is_empty() {
		for (index, size) in unsized_at.into_iter().zip(store.state_sizes(&probes)?) {
			sizes[index] = size;
		}
	}
	Ok((
		OperatorSlice {
			writes,
			checkpoints: checkpoints.to_vec(),
			deletes: deletes.to_vec(),
		},
		sizes,
	))
}

#[cfg(test)]
static LSN: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn next_lsn() -> Lsn {
	Lsn::new(LSN.fetch_add(1, Ordering::Relaxed) + 1)
}

#[cfg(test)]
pub(crate) fn persist(store: &OperatorStore, pending: &Pending) {
	let (slice, sizes) = operator_section(store, pending, &[], &[]).unwrap();
	store.apply_batch_with_checkpoints(&classify_slice(&slice, &sizes), &[], &[], Some(next_lsn())).unwrap();
}
