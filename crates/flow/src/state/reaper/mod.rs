// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::operator::OperatorState;
use reifydb_core::{key::operator_state::GroupStateKey, state::store::StateStore};
use reifydb_value::Result;

use crate::state::expiry::ExpiryIndex;

pub trait Reaps {
	fn reap_keys(&self) -> Vec<GroupStateKey>;
}

pub fn reap_due<S, E>(store: &mut S, index: &mut ExpiryIndex<E>, horizon: u64, batch: usize) -> Result<Vec<E>>
where
	S: StateStore,
	E: OperatorState + Clone + Reaps,
{
	let due = index.due(store, horizon, batch)?;
	let mut reaped = Vec::with_capacity(due.len());
	for (index_key, entry) in due {
		index.drop_key(store, &index_key)?;
		for key in entry.reap_keys() {
			store.state_remove(&key)?;
		}
		reaped.push(entry);
	}
	Ok(reaped)
}
