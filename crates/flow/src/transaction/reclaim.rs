// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, store::MultiVersionRow},
	key::{
		EncodableKey,
		operator_group_state::{
			GroupId, GroupStateKey, Keyspace, group_data_inner_range, group_identity_inner_range,
			keyspace_inner_range,
		},
		operator_state::OperatorStateKey,
	},
	state::horizon::Cutoff,
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyspaceOutcome {
	pub removed: usize,
	pub more: bool,

	pub oldest_survivor: Option<DateTime>,
}

impl KeyspaceOutcome {
	pub const NOTHING: Self = Self {
		removed: 0,
		more: false,
		oldest_survivor: None,
	};
}

impl FlowTransaction {
	pub fn reclaim_group_data(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		limit: usize,
	) -> Result<ReclaimOutcome> {
		reifydb_assertions! {
			assert!(
				!group.is_node_scope(),
				"group id 0 addresses operator scope, which holds the interning dictionary and the id \
				 counter; reclaiming it would erase the table that resolves every other group on \
				 this operator and strand all of their state"
			);
		}
		if group.is_node_scope() {
			return Ok(ReclaimOutcome::NOTHING);
		}
		self.reclaim_range(operator, group_data_inner_range(group), limit)
	}

	pub fn reclaim_group_keyspace(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keyspace: Keyspace,
		cutoff: Cutoff,
		cursor: &mut Option<EncodedKey>,
		limit: usize,
	) -> Result<KeyspaceOutcome> {
		reifydb_assertions! {
			assert!(
				!group.is_node_scope(),
				"group id 0 addresses operator scope, whose keyspaces hold the interning dictionary, \
				 the id counter and the indexes; reclaiming one of them through the per-keyspace \
				 path would erase substrate bookkeeping that every group on this operator depends on"
			);
			assert!(
				keyspace.is_data(),
				"only data keyspaces age; a control keyspace carries the group's identity, and \
				 dropping it here would strand the data it addresses instead of reclaiming it \
				 (keyspace={keyspace:?})"
			);
		}
		if group.is_node_scope() || !keyspace.is_data() || limit == 0 {
			return Ok(KeyspaceOutcome::NOTHING);
		}
		if !keyspace.ages_per_row() {
			let outcome = self.reclaim_range(operator, keyspace_inner_range(group, keyspace), limit)?;
			return Ok(KeyspaceOutcome {
				removed: outcome.removed,
				more: outcome.more,
				oldest_survivor: None,
			});
		}

		let base = keyspace_inner_range(group, keyspace);
		let start = match cursor.clone() {
			Some(resume) => Bound::Excluded(resume),
			None => base.start.clone(),
		};
		let batch = self.state_range(
			operator,
			EncodedKeyRange::new(start, base.end.clone()),
			Some(limit),
			"reclaim::group_keyspace",
		)?;
		let more = batch.has_more;
		let last = batch.items.last().map(Self::inner_key);

		let mut removed = 0;
		let mut oldest_survivor: Option<DateTime> = None;
		for item in &batch.items {
			let written = item.row.updated_at();
			reifydb_assertions! {
				assert!(
					!written.is_epoch(),
					"a row in a per-row keyspace carries no write stamp. Rows are stamped by \
					 whoever encodes them, not by the store, so an unstamped row reads as \
					 written at the epoch and this sweep takes it on the first pass that \
					 reaches its group - deleting live state with no error to show for it. \
					 Every writer of {keyspace:?} must stamp from FlowTransaction::written_at \
					 before the keyspace may opt into Keyspace::ages_per_row"
				);
			}
			if written > cutoff.instant() {
				oldest_survivor = Some(match oldest_survivor {
					Some(current) if current <= written => current,
					_ => written,
				});
				continue;
			}
			self.state_remove(operator, &Self::inner_group_key(item))?;
			removed += 1;
		}

		*cursor = match more {
			true => last,
			false => None,
		};
		Ok(KeyspaceOutcome {
			removed,
			more,
			oldest_survivor,
		})
	}

	fn inner_key(item: &MultiVersionRow) -> EncodedKey {
		EncodedKey::new(
			OperatorStateKey::decode(&item.key).expect("state_range must return OperatorState keys").key,
		)
	}

	fn inner_group_key(item: &MultiVersionRow) -> GroupStateKey {
		GroupStateKey::from_framed(Self::inner_key(item)).expect("operator state rows carry a framed inner key")
	}

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
	use reifydb_codec::{encoded::row::EncodedRow, state::OperatorState};
	use reifydb_core::{
		actors::pending::PendingWrite,
		common::CommitVersion,
		key::operator_group_state::{Keyspace, OperatorGroupStateKey, group_inner_range, keyspace_inner_range},
		state::horizon::Cutoff,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::{datetime::DateTime, duration::Duration, identity::IdentityId};

	use super::*;
	use crate::transaction::ChangeCoordinate;

	const NODE: OperatorId = OperatorId(1);
	const GROUP: GroupId = GroupId(7);
	const NEIGHBOUR: GroupId = GroupId(8);

	// A keyspace the substrate has never heard of, as a custom FFI operator would invent.
	const NOVEL: Keyspace = Keyspace(0x55);

	// The width a 60s retention scale derives (scale / BUCKETS_PER_HORIZON, in nanoseconds); a
	// cutoff must clear whole buckets, not land inside one.
	const BUCKET_WIDTH: u64 = 3_750_000_000;

	// JOIN_LEFT is not a per-row keyspace, so `reclaim_group_keyspace` takes the whole-range path and
	// neither of these is read. They are named rather than inlined so a future reader does not take
	// the values for meaningful bounds on what these tests assert.
	fn ignored_cutoff() -> Cutoff {
		Cutoff(DateTime::MAX)
	}

	fn payload() -> EncodedRow {
		1u64.encode_state(DateTime::EPOCH).unwrap().into_row()
	}

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		);
		// The substrate derives an intern's position from the change coordinate, so it is set here.
		txn.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(0),
			version: CommitVersion(0),
		});
		txn
	}

	fn write(txn: &mut FlowTransaction, group: GroupId, keyspace: Keyspace, suffix: u8) {
		let key = OperatorGroupStateKey::inner_encoded(group, keyspace, vec![suffix]);
		txn.state_set(NODE, &key, payload()).unwrap();
	}

	fn count(txn: &mut FlowTransaction, range: EncodedKeyRange) -> usize {
		txn.state_range(NODE, range, None, "test").unwrap().items.len()
	}

	fn commit_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		// Expresses a crash point: what committed before it survives, what came after never
		// happened, and nothing in RAM carries over into the cold interner that follows.
		let pending = txn.take_pending();
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		for (k, pw) in pending.iter_sorted() {
			match pw {
				PendingWrite::Set(v) => cmd.set(k, v.clone()).unwrap(),
				PendingWrite::Remove {
					announce: true,
				} => cmd.remove(k).unwrap(),
				PendingWrite::Remove {
					announce: false,
				} => cmd.remove_silent(k).unwrap(),
			};
		}
		cmd.commit_unchecked().unwrap();
	}

	fn restarted(engine: &TestEngine) -> FlowTransaction {
		// Registers the operator's horizon so activity is bucketed at the width the scan divides by.
		// Without it a cutoff chosen for one quantisation is compared against another's buckets.
		let txn = deferred(engine);
		txn.group_interner().set_activity_grid(NODE, Some(Duration::from_seconds(60).unwrap()));
		txn
	}

	fn mapping_count(txn: &mut FlowTransaction, group: GroupId) -> usize {
		count(txn, keyspace_inner_range(group, Keyspace::ROW_NUMBER_MAPPING))
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
		// A custom operator can invent any keyspace, but the substrate builds the key so the row
		// still lands inside the group's range and reclamation takes it without knowing it exists.
		// A design where each driver enumerates its own keyspaces leaks a forgotten one forever.
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
	fn reclaiming_one_keyspace_spares_every_other_keyspace_of_the_same_group() {
		// A join key's left and right rows share one group, so different ttls mean reclaiming one
		// keyspace inside a group that must stay whole. Bleeding past the keyspace edge takes the
		// other side's rows: the join keeps probing and finds nothing, with no crash to show for it.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		write(&mut txn, GROUP, Keyspace::JOIN_LEFT, 1);
		write(&mut txn, GROUP, Keyspace::JOIN_LEFT, 2);
		write(&mut txn, GROUP, Keyspace::JOIN_RIGHT, 1);
		seed(&mut txn, GROUP);

		let outcome = txn
			.reclaim_group_keyspace(NODE, GROUP, Keyspace::JOIN_LEFT, ignored_cutoff(), &mut None, 100)
			.unwrap();

		assert_eq!(outcome.removed, 2, "only the two left rows");
		assert!(!outcome.more);
		assert_eq!(count(&mut txn, keyspace_inner_range(GROUP, Keyspace::JOIN_LEFT)), 0);
		assert_eq!(
			count(&mut txn, keyspace_inner_range(GROUP, Keyspace::JOIN_RIGHT)),
			1,
			"the other side of the join must be untouched"
		);
		for keyspace in [Keyspace::ACCUMULATOR, Keyspace::BUFFER, Keyspace::RUNNING, NOVEL] {
			assert_eq!(
				count(&mut txn, keyspace_inner_range(GROUP, keyspace)),
				2,
				"unrelated data keyspace {keyspace:?} lost rows to a per-keyspace reclaim"
			);
		}
		assert_eq!(
			count(&mut txn, keyspace_inner_range(GROUP, Keyspace::ROW_NUMBER_MAPPING)),
			1,
			"identity is not this sweep's business"
		);
	}

	#[test]
	fn reclaiming_one_keyspace_leaves_the_neighbouring_group_alone() {
		// The per-keyspace range is nested inside the per-group range, so a bad upper bound here
		// runs off the end of the group entirely and into the next id's rows.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		write(&mut txn, GROUP, Keyspace::JOIN_LEFT, 1);
		write(&mut txn, NEIGHBOUR, Keyspace::JOIN_LEFT, 1);

		txn.reclaim_group_keyspace(NODE, GROUP, Keyspace::JOIN_LEFT, ignored_cutoff(), &mut None, 100).unwrap();

		assert_eq!(
			count(&mut txn, keyspace_inner_range(NEIGHBOUR, Keyspace::JOIN_LEFT)),
			1,
			"the same keyspace in the adjacent group must survive"
		);
	}

	#[test]
	fn a_per_keyspace_reclaim_reports_more_work_and_resumes_where_it_stopped() {
		// The sweep budgets rows per pass, so a side holding more than the budget must drain across
		// passes. `more` is what tells the caller to come back; reporting false with rows left
		// strands them until the group's own horizon.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		for suffix in 0..5u8 {
			write(&mut txn, GROUP, Keyspace::JOIN_LEFT, suffix);
		}

		let first = txn
			.reclaim_group_keyspace(NODE, GROUP, Keyspace::JOIN_LEFT, ignored_cutoff(), &mut None, 2)
			.unwrap();
		assert_eq!(first.removed, 2);
		assert!(first.more, "three rows are still there");

		let second = txn
			.reclaim_group_keyspace(NODE, GROUP, Keyspace::JOIN_LEFT, ignored_cutoff(), &mut None, 100)
			.unwrap();
		assert_eq!(second.removed, 3);
		assert!(!second.more);
		assert_eq!(count(&mut txn, keyspace_inner_range(GROUP, Keyspace::JOIN_LEFT)), 0);
	}

	#[test]
	fn phase_one_leaves_identity_intact() {
		// Identity outliving data is the point of the two-phase split: a sink row can still name the
		// mapping after the accumulators are gone. Taking the mapping here mints a duplicate row on
		// the group's next wake.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		seed(&mut txn, GROUP);

		txn.reclaim_group_data(NODE, GROUP, 100).unwrap();

		assert_eq!(count(&mut txn, keyspace_inner_range(GROUP, Keyspace::ROW_NUMBER_MAPPING)), 1);
		assert_eq!(count(&mut txn, keyspace_inner_range(GROUP, Keyspace::GROUP_RECORD)), 1);
	}

	#[test]
	fn phase_two_erases_identity_and_stops_the_group_resolving() {
		// After phase 2 nothing of the group may remain, including the dictionary entry that
		// resolves its bytes to an id - a surviving entry is a per-group leak no later pass revisits.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let group_bytes = EncodedKey::new(b"a-group");
		let (id, _) = txn.intern_group(NODE, &group_bytes).unwrap();
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
		// An off-by-one in the range bounds silently destroys a live group's state. The neighbour is
		// the adjacent id precisely because that is where a bad upper bound would bleed.
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
		// The interning dictionary and the id counter live at operator scope. A group range reaching
		// them would erase the address book for every live group on the operator, and the counter with
		// it, letting ids be handed out a second time.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let other = EncodedKey::new(b"still-alive");
		txn.intern_group(NODE, &other).unwrap();
		let (id, _) = txn.intern_group(NODE, &EncodedKey::new(b"doomed")).unwrap();
		seed(&mut txn, id);

		txn.reclaim_group_data(NODE, id, 100).unwrap();

		assert_eq!(
			txn.lookup_group(NODE, &other).unwrap(),
			Some(GroupId::FIRST),
			"another group's dictionary entry must survive"
		);
		let next = txn.intern_group(NODE, &EncodedKey::new(b"after")).unwrap().0;
		assert!(next > id, "the counter must survive so ids keep advancing past the reclaimed one");
	}

	#[test]
	fn reclamation_is_bounded_by_its_limit_and_reports_the_remainder() {
		// Every bulk delete rides the single write mutex, so an unbounded range delete is a latency
		// incident waiting for a high-cardinality group.
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
	#[test]
	fn a_crash_after_erasing_data_but_before_deferring_leaves_the_group_reclaimable() {
		// The data erase commits, then the process dies before defer_group marks the record. The
		// group wakes still in the activity index with a live record, and skipping it there means
		// it never reaches the identity phase and strands its mapping for the life of the operator.
		let engine = TestEngine::new();
		let mut txn = restarted(&engine);
		let bytes = EncodedKey::new(b"crashes-mid-phase-one");
		let (id, _) = txn.intern_group(NODE, &bytes).unwrap();
		seed(&mut txn, id);
		txn.reclaim_group_data(NODE, id, 100).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = restarted(&engine);
		assert_eq!(mapping_count(&mut txn, id), 1, "the mapping is identity and must survive phase 1");
		assert_eq!(
			txn.due_groups(NODE, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10).unwrap(),
			vec![id],
			"a group whose defer never committed must still be offered to the data phase"
		);

		let outcome = txn.reclaim_group_data(NODE, id, 100).unwrap();
		assert_eq!(outcome.removed, 0, "the replayed erase finds nothing left and must be harmless");
		assert!(txn.defer_group(NODE, id).unwrap());
		assert_eq!(
			txn.due_identity_groups(NODE, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10).unwrap(),
			vec![id]
		);
	}

	#[test]
	fn a_crash_between_the_phases_keeps_the_mapping_until_the_identity_phase_runs() {
		// The gap between the phases is a long horizon, so a restart inside it is the common case.
		// The reclaimed marker lives in the durable record so the woken process still knows the data
		// is gone and the identity is not; losing it re-runs phase 1 forever or takes the mapping early.
		let engine = TestEngine::new();
		let mut txn = restarted(&engine);
		let bytes = EncodedKey::new(b"crashes-between-phases");
		let (id, _) = txn.intern_group(NODE, &bytes).unwrap();
		seed(&mut txn, id);
		txn.reclaim_group_data(NODE, id, 100).unwrap();
		txn.defer_group(NODE, id).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = restarted(&engine);
		assert_eq!(mapping_count(&mut txn, id), 1, "the mapping must outlive the data across a restart");
		assert!(
			txn.due_groups(NODE, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10).unwrap().is_empty(),
			"a deferred group must not be handed back to the data phase after a restart"
		);
		assert_eq!(
			txn.due_identity_groups(NODE, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10).unwrap(),
			vec![id]
		);

		txn.reclaim_group_identity(NODE, id, 100).unwrap();
		assert_eq!(count(&mut txn, group_inner_range(id)), 0);
	}

	#[test]
	fn a_half_drained_group_resumes_where_it_stopped_after_a_crash() {
		// A budget-bounded erase can be interrupted at any point. Marking the group reclaimed while
		// rows remain stops the data scan offering it, and the identity phase then deletes the
		// record that addresses the survivors, leaking them with no way back.
		let engine = TestEngine::new();
		let mut txn = restarted(&engine);
		let bytes = EncodedKey::new(b"crashes-mid-drain");
		let (id, _) = txn.intern_group(NODE, &bytes).unwrap();
		seed(&mut txn, id);
		let partial = txn.reclaim_group_data(NODE, id, 3).unwrap();
		assert!(partial.more, "precondition: the budget must have left rows behind");
		commit_pending(&engine, &mut txn);

		let mut txn = restarted(&engine);
		assert_eq!(
			txn.due_groups(NODE, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10).unwrap(),
			vec![id],
			"a half-drained group must come back to the data phase, not the identity phase"
		);
		assert!(
			txn.due_identity_groups(NODE, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10)
				.unwrap()
				.is_empty(),
			"and must never be identity-due while it still holds data"
		);

		let mut outcome = txn.reclaim_group_data(NODE, id, 100).unwrap();
		assert_eq!(outcome.removed, 5, "the resumed erase takes exactly the rows the first pass left");
		assert!(!outcome.more);
		outcome = txn.reclaim_group_data(NODE, id, 100).unwrap();
		assert_eq!(outcome.removed, 0);
	}

	#[test]
	fn the_identity_phase_never_leaves_a_dictionary_entry_behind_its_rows() {
		// The identity range and the dictionary clear in one transaction, so no crash can commit the
		// first without the second. Otherwise the group resolves to an id whose record and mapping
		// are gone, and phase 2 can never find it again to finish the job.
		let engine = TestEngine::new();
		let mut txn = restarted(&engine);
		let bytes = EncodedKey::new(b"atomic-identity");
		let (id, _) = txn.intern_group(NODE, &bytes).unwrap();
		seed(&mut txn, id);
		txn.reclaim_group_data(NODE, id, 100).unwrap();
		txn.defer_group(NODE, id).unwrap();
		txn.reclaim_group_identity(NODE, id, 100).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = restarted(&engine);
		assert_eq!(count(&mut txn, group_inner_range(id)), 0, "no row of the group may survive the restart");
		assert_eq!(
			txn.lookup_group(NODE, &bytes).unwrap(),
			None,
			"and the dictionary must not still resolve bytes whose rows are gone"
		);

		let (reborn, is_new) = txn.intern_group(NODE, &bytes).unwrap();
		assert!(is_new, "the key is unknown again, so it must mint afresh");
		assert_ne!(reborn, id, "a reclaimed id must never be handed back out");
	}
}
