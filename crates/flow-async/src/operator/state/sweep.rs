// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	internal_err,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceMask},
	state::timer::StateStore,
};
use reifydb_value::Result;

pub fn sweep_all<S: StateStore + ?Sized>(
	store: &mut S,
	groups: &[GroupId],
	keyspaces: KeyspaceMask,
) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
	let sweep = store.group_sweep_many(groups, usize::MAX, keyspaces)?;
	if !sweep.complete {
		return internal_err!("a group sweep stopped before the end of its groups");
	}
	Ok(sweep.rows)
}
