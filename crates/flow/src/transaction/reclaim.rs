// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_group_state::{GroupId, GroupStateKey, group_identity_inner_range},
		operator_state::OperatorStateKey,
	},
};
use reifydb_value::{Result, reifydb_assertions};

use super::FlowTransaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimOutcome {
	pub removed: usize,
	pub more: bool,
}

impl ReclaimOutcome {
	pub const NOTHING: Self = Self {
		removed: 0,
		more: false,
	};
}

impl FlowTransaction {
	pub fn reclaim_group_identity(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		reifydb_assertions! {
			assert!(
				!group.is_node_scope(),
				"group id 0 addresses operator scope; reclaiming its identity would delete the \
				 interning dictionary itself"
			);
		}
		if group.is_node_scope() {
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
				GroupStateKey::from_framed(EncodedKey::new(decoded.key))
					.expect("operator state rows carry a framed inner key")
			})
			.collect();
		let removed = keys.len();
		for key in &keys {
			self.state_remove(operator, key)?;
		}
		Ok(ReclaimOutcome {
			removed,
			more: batch.has_more,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_codec::row::operator::{EncodedOperatorRow, OperatorState};
	use reifydb_core::{
		actors::pending::{Pending, PendingLayers},
		common::CommitVersion,
		interface::catalog::flow::OperatorId,
		key::operator_group_state::{Keyspace, OperatorGroupStateKey, group_inner_range},
		state::budget::OperatorStateBudgetHandle,
	};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::{datetime::DateTime, identity::IdentityId};

	use super::*;
	use crate::transaction::{ChangeCoordinate, DeferredParams, substrate::FlowSubstrate};

	const NODE: OperatorId = OperatorId(1);

	fn payload() -> EncodedOperatorRow {
		1u64.encode_state(DateTime::EPOCH).unwrap()
	}

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: Pending::new(),
			base_pending: PendingLayers::empty(),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			single: parent.single.clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			substrate: FlowSubstrate {
				operators: engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
			state_budget: OperatorStateBudgetHandle::default(),
		});
		// The substrate derives an intern's position from the change coordinate, so it is set here.
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
			version: CommitVersion(0),
		});
		txn
	}

	fn seed_identity(txn: &mut FlowTransaction, id: GroupId) {
		write(txn, id, Keyspace::GROUP_RECORD, 1);
		write(txn, id, Keyspace::ROW_NUMBER_MAPPING, 1);
	}

	fn write(txn: &mut FlowTransaction, group: GroupId, keyspace: Keyspace, suffix: u8) {
		let key = OperatorGroupStateKey::inner_encoded(group, keyspace, vec![suffix]);
		txn.state_set(NODE, &key, payload()).unwrap();
	}

	fn count(txn: &mut FlowTransaction, range: EncodedKeyRange) -> usize {
		txn.state_range(NODE, range, None, "test").unwrap().items.len()
	}

	#[test]
	fn the_identity_reclaim_erases_the_range_and_stops_the_group_resolving() {
		// The append operator erases a removed source row's identity inline, so a surviving
		// dictionary entry would let the next event on the same key resolve to a group whose
		// record and mapping are gone, stranding it forever.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let group_bytes = EncodedKey::new(b"a-group");
		let (id, _) = txn.intern_group(NODE, &group_bytes).unwrap();
		seed_identity(&mut txn, id);

		let outcome = txn.reclaim_group_identity(NODE, id, 100).unwrap();

		assert_eq!(outcome.removed, 3, "the substrate record, the seeded record row and the mapping");
		assert!(!outcome.more);
		assert_eq!(count(&mut txn, group_inner_range(id)), 0, "the group's range must be empty");
		assert_eq!(
			txn.lookup_group(NODE, &group_bytes).unwrap(),
			None,
			"the dictionary entry must go with the identity"
		);
	}

	#[test]
	fn a_bounded_identity_reclaim_keeps_the_dictionary_until_the_range_is_drained() {
		// The append remove path runs with a small fixed limit on the apply hot path. Dropping the
		// dictionary entry while identity rows remain would strand them: nothing could resolve the
		// group to finish the erase.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let group_bytes = EncodedKey::new(b"chunky");
		let (id, _) = txn.intern_group(NODE, &group_bytes).unwrap();
		for suffix in 0..4u8 {
			write(&mut txn, id, Keyspace::ROW_NUMBER_MAPPING, suffix);
		}

		let partial = txn.reclaim_group_identity(NODE, id, 2).unwrap();
		assert_eq!(partial.removed, 2);
		assert!(partial.more, "the caller must learn the group is not drained");
		assert_eq!(
			txn.lookup_group(NODE, &group_bytes).unwrap(),
			Some(id),
			"a half-drained group must still resolve so a later pass can finish it"
		);

		let rest = txn.reclaim_group_identity(NODE, id, 100).unwrap();
		assert!(!rest.more);
		assert_eq!(txn.lookup_group(NODE, &group_bytes).unwrap(), None);
	}

	#[test]
	fn reclaiming_one_groups_identity_leaves_its_neighbour_untouched() {
		// An off-by-one in the identity range bounds silently destroys a live group's mapping. The
		// neighbour is the adjacent id precisely because that is where a bad upper bound would bleed.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let (id, _) = txn.intern_group(NODE, &EncodedKey::new(b"doomed")).unwrap();
		let (neighbour, _) = txn.intern_group(NODE, &EncodedKey::new(b"alive")).unwrap();
		seed_identity(&mut txn, id);
		seed_identity(&mut txn, neighbour);

		txn.reclaim_group_identity(NODE, id, 100).unwrap();

		assert_eq!(count(&mut txn, group_inner_range(id)), 0);
		assert_eq!(count(&mut txn, group_inner_range(neighbour)), 3, "the neighbour must be whole");
		assert_eq!(txn.lookup_group(NODE, &EncodedKey::new(b"alive")).unwrap(), Some(neighbour));
	}

	#[test]
	fn a_reclaimed_group_reborn_mints_a_fresh_id() {
		// A reclaimed id handed back out would collide the reborn key's state with any stale rows
		// still addressed by the old id.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let bytes = EncodedKey::new(b"reborn");
		let (id, _) = txn.intern_group(NODE, &bytes).unwrap();
		seed_identity(&mut txn, id);
		txn.reclaim_group_identity(NODE, id, 100).unwrap();

		let (reborn, is_new) = txn.intern_group(NODE, &bytes).unwrap();

		assert!(is_new, "the key is unknown again, so it must mint afresh");
		assert_ne!(reborn, id, "a reclaimed id must never be handed back out");
	}
}
