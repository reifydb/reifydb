// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator::state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey, keyspace_inner_range},
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
		self.reclaim_identity_keyspaces(operator, group, limit)
	}

	fn reclaim_group_identity_keys(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[GroupStateKey],
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
		let mut removed = 0u64;
		for key in keys {
			let carried = OperatorStateKey::decode_inner(key.as_encoded().as_bytes()).map(|(id, _, _)| id);
			reifydb_assertions! {
				assert!(
					carried == Some(group),
					"reclaiming group {} was handed a key stamped group {:?}; removing it would delete state belonging to another partition",
					group,
					carried
				);
			}
			if carried != Some(group) {
				continue;
			}
			self.state_remove(operator, key)?;
			removed += 1;
		}
		Ok(ReclaimOutcome {
			removed: Count::new(removed),
			more: false,
		})
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
		let mut removed = 0u64;
		let mut more = false;
		for id in (u8::MIN..=u8::MAX).rev() {
			let keyspace = KeyspaceId(id);
			if !keyspace.is_identity() || !keyspace.is_known() {
				continue;
			}
			if removed as usize >= limit {
				more = true;
				break;
			}
			let range = keyspace_inner_range(group, keyspace);
			let query = StateRange::forward(range, "reclaim::keyspace").limit(limit - removed as usize);
			let batch = self.state_range(operator, query)?;
			more |= batch.has_more;
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
			removed += keys.len() as u64;
			for key in &keys {
				self.state_remove(operator, key)?;
			}
		}
		Ok(ReclaimOutcome {
			removed: Count::new(removed),
			more,
		})
	}
}

impl<T: StateExtension> ReclaimExtension for T {}
