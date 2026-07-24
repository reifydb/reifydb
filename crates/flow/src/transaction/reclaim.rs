// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::{
		EncodableKey,
		flow_node_internal_state::FlowNodeInternalStateKey,
		operator_state::{GroupId, group_data_inner_range, group_identity_inner_range},
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
	pub fn reclaim_group_data(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		reifydb_assertions! {
			assert!(
				!group.is_node_scope(),
				"group id 0 addresses node scope, which holds the interning dictionary and the id \
				 counter; reclaiming it would erase the table that resolves every other group on \
				 this node and strand all of their state"
			);
		}
		if group.is_node_scope() {
			return Ok(ReclaimOutcome::NOTHING);
		}
		self.reclaim_range(node, group_data_inner_range(group), limit)
	}

	pub fn reclaim_group_identity(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		reifydb_assertions! {
			assert!(
				!group.is_node_scope(),
				"group id 0 addresses node scope; reclaiming its identity would delete the \
				 interning dictionary itself"
			);
		}
		if group.is_node_scope() {
			return Ok(ReclaimOutcome::NOTHING);
		}
		let group_bytes = self.group_bytes(node, group)?;
		let outcome = self.reclaim_range(node, group_identity_inner_range(group), limit)?;
		if !outcome.more && let Some(bytes) = group_bytes {
			self.forget_group(node, &bytes)?;
		}
		Ok(outcome)
	}

	fn reclaim_range(
		&mut self,
		node: FlowNodeId,
		range: EncodedKeyRange,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		if limit == 0 {
			return Ok(ReclaimOutcome::NOTHING);
		}
		let batch = self.internal_state_range(node, range, Some(limit))?;
		let keys: Vec<EncodedKey> = batch
			.items
			.iter()
			.map(|item| {
				let decoded = FlowNodeInternalStateKey::decode(&item.key)
					.expect("internal_state_range must return FlowNodeInternalState keys");
				EncodedKey::new(decoded.key)
			})
			.collect();
		let removed = keys.len();
		for key in &keys {
			self.internal_state_remove(node, key)?;
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
	use reifydb_codec::{encoded::row::EncodedRow, state::OperatorState};
	use reifydb_core::key::operator_state::{
		Keyspace, OperatorStateKey, group_inner_range, keyspace_inner_range,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	const NODE: FlowNodeId = FlowNodeId(1);
	const GROUP: GroupId = GroupId(7);
	const NEIGHBOUR: GroupId = GroupId(8);

	// A keyspace the substrate has never heard of, as a custom FFI operator would invent.
	const NOVEL: Keyspace = Keyspace(0x55);

	fn payload() -> EncodedRow {
		1u64.encode_state(0).unwrap().into_row()
	}

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		)
	}

	fn write(txn: &mut FlowTransaction, group: GroupId, keyspace: Keyspace, suffix: u8) {
		let key = OperatorStateKey::inner_encoded(group, keyspace, vec![suffix]);
		txn.internal_state_set(NODE, &key, payload()).unwrap();
	}

	fn count(txn: &mut FlowTransaction, range: EncodedKeyRange) -> usize {
		txn.internal_state_range(NODE, range, None).unwrap().items.len()
	}

	fn seed(txn: &mut FlowTransaction, group: GroupId) {
		for keyspace in [Keyspace::ACCUMULATOR, Keyspace::BUFFER, Keyspace::RUNNING, NOVEL] {
			write(txn, group, keyspace, 1);
			write(txn, group, keyspace, 2);
		}
		write(txn, group, Keyspace::GROUP_RECORD, 1);
		write(txn, group, Keyspace::ROW_NUMBER_MAPPING, 1);
	}

	#[test]
	fn phase_one_erases_every_data_keyspace_including_one_the_substrate_has_never_heard_of() {
		// This is the property the whole group-major codec was chosen for. A custom operator can
		// invent any keyspace it likes; because the key is built by the substrate the row still lands
		// inside the group's range, so reclamation takes it without knowing it exists. The previous
		// design needed each driver to enumerate its own keyspaces, and a forgotten one leaked
		// forever - which is exactly how gap G2 happened.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed(&mut txn, GROUP);
		assert_eq!(count(&mut txn, keyspace_inner_range(GROUP, NOVEL)), 2, "precondition: novel rows exist");

		let outcome = txn.reclaim_group_data(NODE, GROUP, 100).unwrap();

		assert_eq!(outcome.removed, 8, "four data keyspaces of two rows each");
		assert!(!outcome.more);
		for keyspace in [Keyspace::ACCUMULATOR, Keyspace::BUFFER, Keyspace::RUNNING, NOVEL] {
			assert_eq!(
				count(&mut txn, keyspace_inner_range(GROUP, keyspace)),
				0,
				"data keyspace {keyspace:?} survived phase 1"
			);
		}
	}

	#[test]
	fn phase_one_leaves_identity_intact() {
		// Identity outliving data is the entire point of the two-phase split: a sink row can still
		// name the mapping after the accumulators are gone. Taking the mapping here would mint a
		// duplicate row on the group's next wake (landmine L2).
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed(&mut txn, GROUP);

		txn.reclaim_group_data(NODE, GROUP, 100).unwrap();

		assert_eq!(count(&mut txn, keyspace_inner_range(GROUP, Keyspace::ROW_NUMBER_MAPPING)), 1);
		assert_eq!(count(&mut txn, keyspace_inner_range(GROUP, Keyspace::GROUP_RECORD)), 1);
	}

	#[test]
	fn phase_two_erases_identity_and_stops_the_group_resolving() {
		// After phase 2 nothing of the group may remain anywhere: not its identity rows, and not the
		// dictionary entry that resolves its bytes to an id. A surviving dictionary entry is a
		// per-group leak that no later pass would ever revisit.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let group_bytes = EncodedKey::new(b"a-group".to_vec());
		let (id, _) = txn.intern_group(NODE, &group_bytes, 0).unwrap();
		seed(&mut txn, id);

		txn.reclaim_group_data(NODE, id, 100).unwrap();
		let outcome = txn.reclaim_group_identity(NODE, id, 100).unwrap();

		assert_eq!(outcome.removed, 3, "the substrate record, the seeded record row and the mapping");
		assert_eq!(count(&mut txn, group_inner_range(id)), 0, "the group's range must be empty");
		assert_eq!(
			txn.lookup_group(NODE, &group_bytes).unwrap(),
			None,
			"the dictionary entry must go with the identity phase"
		);
	}

	#[test]
	fn reclaiming_one_group_leaves_its_neighbour_untouched() {
		// Ranges are the mechanism, so an off-by-one in the bounds destroys a live group's state
		// silently. The neighbour is the adjacent id precisely because that is where a bad upper
		// bound would bleed.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed(&mut txn, GROUP);
		seed(&mut txn, NEIGHBOUR);

		txn.reclaim_group_data(NODE, GROUP, 100).unwrap();
		txn.reclaim_group_identity(NODE, GROUP, 100).unwrap();

		assert_eq!(count(&mut txn, group_inner_range(GROUP)), 0, "the reclaimed group is gone");
		assert_eq!(count(&mut txn, group_inner_range(NEIGHBOUR)), 10, "the neighbour must be whole");
	}

	#[test]
	fn reclaiming_a_group_never_touches_node_scope() {
		// The interning dictionary and the id counter live at node scope. If a group range reached
		// them, reclaiming one dead group would erase the address book for every live group on the
		// node - and the counter, letting ids be handed out a second time.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let other = EncodedKey::new(b"still-alive".to_vec());
		txn.intern_group(NODE, &other, 0).unwrap();
		let (id, _) = txn.intern_group(NODE, &EncodedKey::new(b"doomed".to_vec()), 0).unwrap();
		seed(&mut txn, id);

		txn.reclaim_group_data(NODE, id, 100).unwrap();

		assert_eq!(
			txn.lookup_group(NODE, &other).unwrap(),
			Some(GroupId::FIRST),
			"another group's dictionary entry must survive"
		);
		let next = txn.intern_group(NODE, &EncodedKey::new(b"after".to_vec()), 0).unwrap().0;
		assert!(next > id, "the counter must survive so ids keep advancing past the reclaimed one");
	}

	#[test]
	fn reclamation_is_bounded_by_its_limit_and_reports_the_remainder() {
		// Landmine L10: every bulk delete rides the single write mutex, so an unbounded range delete
		// is a latency incident waiting for a high-cardinality group. The caller must be able to take
		// a slice and be told there is more to do.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed(&mut txn, GROUP);

		let first = txn.reclaim_group_data(NODE, GROUP, 3).unwrap();
		assert_eq!(first.removed, 3, "a slice must remove exactly its limit when more remain");
		assert!(first.more, "the caller must learn that the group is not drained");

		let mut drained = first.removed;
		let mut outcome = first;
		while outcome.more {
			outcome = txn.reclaim_group_data(NODE, GROUP, 3).unwrap();
			drained += outcome.removed;
		}

		assert_eq!(drained, 8, "successive slices must drain the group exactly once, with no gaps");
		assert_eq!(count(&mut txn, group_inner_range(GROUP)), 2, "only the identity rows remain");
	}

	#[test]
	fn a_zero_limit_does_no_work() {
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed(&mut txn, GROUP);

		let outcome = txn.reclaim_group_data(NODE, GROUP, 0).unwrap();

		assert_eq!(outcome, ReclaimOutcome::NOTHING);
		assert_eq!(count(&mut txn, group_inner_range(GROUP)), 10, "a zero budget must not delete anything");
	}
}
