// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		any::TaggedKey,
		operator::state::{GroupId, GroupStateKey, group_identity_inner_range},
	},
};
use reifydb_value::{Result, count::Count, reifydb_assertions};

use crate::{
	operator::state::reclaim::ReclaimOutcome,
	transaction::state::{StateExtension, StateRange},
};

pub trait ReclaimExtension: StateExtension {
	fn reclaim_group_identity(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		reifydb_assertions! {
			assert!(
				!group.is_root(),
				"group id 0 is the root group; reclaiming its identity would delete the timer wheel, the expiry index and the reap queue"
			);
		}
		if group.is_root() {
			return Ok(ReclaimOutcome::NOTHING);
		}
		let outcome = self.reclaim_identity_keyspaces(operator, group, limit)?;
		self.operator_store().invalidate_group(operator, group)?;
		Ok(outcome)
	}

	fn reclaim_identity_keyspaces(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		if limit == 0 {
			return Ok(ReclaimOutcome::NOTHING);
		}
		let query = StateRange::forward(group_identity_inner_range(group), "reclaim::keyspace").limit(limit);
		let batch = self.state_range(operator, query)?;
		let keys: Vec<GroupStateKey> = batch
			.items
			.iter()
			.map(|item| {
				let TaggedKey::OperatorState(decoded) = &item.key else {
					panic!("state_range must return OperatorState keys");
				};
				GroupStateKey::from_framed(decoded.inner())
					.expect("operator state rows carry a framed inner key")
			})
			.collect();
		self.state_remove_many(operator, &keys)?;
		Ok(ReclaimOutcome {
			removed: Count::new(keys.len() as u64),
			more: batch.has_more,
		})
	}
}

impl<T: StateExtension> ReclaimExtension for T {}
