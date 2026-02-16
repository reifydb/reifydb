// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::reducer::{ReducerActionId, ReducerId};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::store::sequence::{
	generator::u64::GeneratorU64,
	system::{REDUCER_ACTION_KEY, REDUCER_KEY},
};

pub(crate) fn next_reducer_id(txn: &mut AdminTransaction) -> crate::Result<ReducerId> {
	GeneratorU64::next(txn, &REDUCER_KEY, None).map(ReducerId)
}

pub(crate) fn next_reducer_action_id(txn: &mut AdminTransaction) -> crate::Result<ReducerActionId> {
	GeneratorU64::next(txn, &REDUCER_ACTION_KEY, None).map(ReducerActionId)
}
