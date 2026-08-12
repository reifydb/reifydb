// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKeyRange;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, OperatorStateKey, group_identity_inner_range},
	},
};
use reifydb_value::{Result, count::Count, reifydb_assertions};

use crate::{
	state::reclaim::ReclaimOutcome,
	transaction::{group::GroupTxn, state::StateTxn},
};

pub trait ReclaimTxn: StateTxn + GroupTxn {
	fn reclaim_group_identity(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		reifydb_assertions! {
			assert!(
				!group.is_root(),
				"group id 0 is the root group; reclaiming its identity would delete the interning dictionary itself"
			);
		}
		if group.is_root() {
			return Ok(ReclaimOutcome::NOTHING);
		}
		let group_bytes = self.group_bytes(operator, group)?;
		let outcome = self.reclaim_range(operator, group_identity_inner_range(group), limit)?;
		if !outcome.more
			&& let Some(bytes) = group_bytes
		{
			self.forget_group(operator, &bytes)?;
		}
		Ok(outcome)
	}

	fn reclaim_range(
		&mut self,
		operator: OperatorId,
		range: EncodedKeyRange,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		if limit == 0 {
			return Ok(ReclaimOutcome::NOTHING);
		}
		let batch = self.state_range(operator, range, Some(limit), "reclaim::range")?;
		let keys: Vec<GroupStateKey> = batch
			.items
			.iter()
			.map(|item| {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				GroupStateKey::from_framed(decoded.inner())
					.expect("operator state rows carry a framed inner key")
			})
			.collect();
		let removed = Count::new(keys.len() as u64);
		for key in &keys {
			self.state_remove(operator, key)?;
		}
		Ok(ReclaimOutcome {
			removed,
			more: batch.has_more,
		})
	}
}

impl<T: StateTxn + GroupTxn> ReclaimTxn for T {}
