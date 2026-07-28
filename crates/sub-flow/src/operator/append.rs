// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin, Diff},
	},
	key::operator_state::GroupId,
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_value::{Result, error::Error, reifydb_assertions, value::row_number::RowNumber};

use crate::{error::FlowGraphError, operator::OperatorCell};

const CAPABILITIES: &[OperatorCapability] = &[
	OperatorCapability::Insert,
	OperatorCapability::Update,
	OperatorCapability::Delete,
	OperatorCapability::Reclaim,
];

const REMOVE_RECLAIM_LIMIT: usize = 8;

pub struct AppendOperator {
	node: FlowNodeId,

	parents: Vec<OperatorCell>,

	input_nodes: Vec<FlowNodeId>,
}

impl AppendOperator {
	pub fn new(node: FlowNodeId, parents: Vec<OperatorCell>, input_nodes: Vec<FlowNodeId>) -> Self {
		reifydb_assertions! {
			assert_eq!(parents.len(), input_nodes.len());
			assert!(parents.len() >= 2, "Append requires at least 2 inputs");
		}

		Self {
			node,
			parents,
			input_nodes,
		}
	}

	#[cfg(test)]
	pub(crate) fn new_for_state_tests(node: FlowNodeId) -> Self {
		Self {
			node,
			parents: Vec::new(),
			input_nodes: Vec::new(),
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parents[0].output_schema()
	}

	fn parent_index_for_origin(&self, origin: &ChangeOrigin) -> Option<usize> {
		match origin {
			ChangeOrigin::Flow(from_node) => self.input_nodes.iter().position(|n| n == from_node),
			ChangeOrigin::Object(_) => None,
		}
	}

	fn group_bytes(parent_index: u8, source_row: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(parent_index);
		serializer.extend_u64(source_row.0);
		serializer.finish()
	}

	fn group_keys(parent_index: usize, source: &Columns) -> Vec<EncodedKey> {
		(0..source.row_count())
			.map(|row_idx| Self::group_bytes(parent_index as u8, source.row_numbers()[row_idx]))
			.collect()
	}

	fn mapping_key() -> EncodedKey {
		EncodedKey::new(Vec::new())
	}
}

impl Operator for AppendOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let parent_origin = change.origin.clone();
		let mut result_diffs = Vec::with_capacity(change.diffs.len());

		for diff in change.diffs {
			let diff_origin = diff.origin().cloned().unwrap_or_else(|| parent_origin.clone());
			let parent_index = self.parent_index_for_origin(&diff_origin).ok_or_else(|| {
				Error::from(FlowGraphError::UnknownDiffOrigin {
					operator: "Append",
					origin: Some(format!("{:?}", diff_origin)),
				})
			})?;
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					if let Some(d) = self.translate_append_insert(txn, parent_index, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					if let Some(d) = self.translate_append_update(txn, parent_index, pre, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					if let Some(d) = self.translate_append_remove(txn, parent_index, pre)? {
						result_diffs.push(d);
					}
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, result_diffs, change.changed_at))
	}
}

impl AppendOperator {
	#[inline]
	fn translate_create_row_numbers(
		&self,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Vec<RowNumber>> {
		let interned = txn.intern_groups(self.node, groups)?;
		let mut output_row_numbers = Vec::with_capacity(interned.len());
		for (group, _) in interned {
			let (output_row_number, _) =
				txn.get_or_create_row_number(self.node, group, &Self::mapping_key())?;
			output_row_numbers.push(output_row_number);
		}
		Ok(output_row_numbers)
	}

	#[inline]
	fn lookup_row_numbers(
		&self,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Option<(Vec<RowNumber>, Vec<GroupId>)>> {
		let mut output_row_numbers = Vec::with_capacity(groups.len());
		let mut ids = Vec::with_capacity(groups.len());
		for group_bytes in groups {
			let Some(group) = txn.lookup_group(self.node, group_bytes)? else {
				return Ok(None);
			};
			let Some(row_number) = txn.get_row_number(self.node, group, &Self::mapping_key())? else {
				return Ok(None);
			};
			output_row_numbers.push(row_number);
			ids.push(group);
		}
		Ok(Some((output_row_numbers, ids)))
	}

	#[inline]
	fn translate_append_insert(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		post: Columns,
	) -> Result<Option<Diff>> {
		if post.row_count() == 0 {
			return Ok(None);
		}
		let groups = Self::group_keys(parent_index, &post);
		let output_row_numbers = self.translate_create_row_numbers(txn, &groups)?;
		let output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::insert(output)))
	}

	#[inline]
	fn translate_append_update(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		pre: Columns,
		post: Columns,
	) -> Result<Option<Diff>> {
		if post.row_count() == 0 {
			return Ok(None);
		}
		let groups = Self::group_keys(parent_index, &pre);
		let Some((output_row_numbers, _)) = self.lookup_row_numbers(txn, &groups)? else {
			return Ok(None);
		};
		txn.intern_groups(self.node, &groups)?;
		let pre_output = pre.with_row_numbers(output_row_numbers.clone());
		let post_output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::update(pre_output, post_output)))
	}

	#[inline]
	fn translate_append_remove(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		pre: Columns,
	) -> Result<Option<Diff>> {
		if pre.row_count() == 0 {
			return Ok(None);
		}
		let groups = Self::group_keys(parent_index, &pre);
		let Some((output_row_numbers, ids)) = self.lookup_row_numbers(txn, &groups)? else {
			return Ok(None);
		};
		for group in ids {
			txn.reclaim_group_identity(self.node, group, REMOVE_RECLAIM_LIMIT)?;
		}
		let output = pre.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::remove(output)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		key::operator_state::group_inner_range,
		state::horizon::{Cutoff, Horizon},
		value::column::columns::Columns,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::value::duration::Duration;

	use super::*;

	const BUCKET_WIDTH: u64 = 4_096;

	fn op(node: u64) -> AppendOperator {
		AppendOperator::new_for_state_tests(FlowNodeId(node))
	}

	// Mirrors register.rs, which registers each node's horizon with the interner. Without it the
	// node falls back to the interner's default bucket width and stamps in no particular domain.
	fn txn_at(engine: &TestEngine, node: FlowNodeId, version: u64) -> FlowTransaction {
		let txn = engine.flow_txn().at(CommitVersion(version)).deferred();
		txn.group_interner().set_horizon(node, Horizon::idle(Duration::from_seconds(60).unwrap()));
		txn
	}

	fn rows(source_rows: &[u64]) -> Columns {
		Columns::empty().with_row_numbers(source_rows.iter().map(|r| RowNumber(*r)).collect())
	}

	fn group_of(txn: &mut FlowTransaction, op: &AppendOperator, parent: u8, source_row: u64) -> Option<GroupId> {
		txn.lookup_group(op.node, &AppendOperator::group_bytes(parent, RowNumber(source_row))).unwrap()
	}

	fn group_rows(txn: &mut FlowTransaction, op: &AppendOperator, group: GroupId) -> usize {
		txn.state_range(op.node, group_inner_range(group), None).unwrap().items.len()
	}

	#[test]
	fn a_source_row_interns_a_group_that_carries_its_output_row_number() {
		// The mapping is the whole reason append holds state, and after the migration it lives at
		// the group's own address rather than at node scope. That is what puts it inside the range
		// phase 2 deletes: a mapping written outside the group would be invisible to reclamation and
		// would leak one row per source row for the life of the node.
		let engine = TestEngine::new();
		let op = op(1);
		let mut txn = txn_at(&engine, op.node, 100);

		let assigned = op.translate_append_insert(&mut txn, 0, rows(&[42])).unwrap().unwrap();
		let Diff::Insert {
			post,
			..
		} = assigned
		else {
			panic!("an insert must translate to an insert");
		};

		let group = group_of(&mut txn, &op, 0, 42).expect("the source row must have interned a group");
		assert_eq!(
			txn.get_row_number(op.node, group, &AppendOperator::mapping_key()).unwrap(),
			Some(post.row_numbers()[0]),
			"the output row number must be readable from inside the group that owns it"
		);
	}

	#[test]
	fn the_same_source_row_always_translates_to_the_same_output_row() {
		// Append's entire contract: a source row keeps one identity downstream for as long as the
		// mapping lives. A second insert that minted a fresh number would duplicate the row in the
		// sink rather than replace it.
		let engine = TestEngine::new();
		let op = op(2);
		let mut txn = txn_at(&engine, op.node, 100);

		let first =
			op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[7]))).unwrap();
		let second =
			op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[7]))).unwrap();

		assert_eq!(first, second, "an already-interned source row must resolve to its existing output row");
	}

	#[test]
	fn each_input_numbers_its_own_source_rows_independently() {
		// The inputs of a union number their rows independently, so row 7 arrives from both. The
		// parent index is in the group bytes precisely so those two are different groups; sharing one
		// would collapse two unrelated source rows onto a single output row and let either input's
		// reclamation erase the other's mapping.
		let engine = TestEngine::new();
		let op = op(3);
		let mut txn = txn_at(&engine, op.node, 100);

		let left =
			op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[7]))).unwrap();
		let right =
			op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(1, &rows(&[7]))).unwrap();

		assert_ne!(
			group_of(&mut txn, &op, 0, 7),
			group_of(&mut txn, &op, 1, 7),
			"the same source row number on two inputs must not share a group"
		);
		assert_ne!(left, right, "and must not share an output row number");
	}

	#[test]
	fn a_source_row_that_was_never_seen_is_looked_up_never_interned() {
		// An update or remove for a row append holds nothing for must not mint anything. If lookup
		// interned, every unmatched diff would leave behind a dictionary entry, a group record and an
		// activity-index row addressing a mapping that does not exist - unbounded growth driven
		// entirely by traffic the operator drops on the floor.
		let engine = TestEngine::new();
		let op = op(4);
		let mut txn = txn_at(&engine, op.node, 100);

		assert!(op.translate_append_update(&mut txn, 0, rows(&[99]), rows(&[99])).unwrap().is_none());
		assert!(op.translate_append_remove(&mut txn, 0, rows(&[99])).unwrap().is_none());

		assert_eq!(group_of(&mut txn, &op, 0, 99), None, "a lookup must not have interned the missing row");
	}

	#[test]
	fn a_partly_known_batch_translates_to_nothing_at_all() {
		// The diff carries one Columns for the whole batch, so it is all-or-nothing: emitting the rows
		// that did resolve would hand the sink a Columns whose row numbers no longer line up with the
		// values beside them.
		let engine = TestEngine::new();
		let op = op(5);
		let mut txn = txn_at(&engine, op.node, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[1]))).unwrap();

		assert!(op.translate_append_remove(&mut txn, 0, rows(&[1, 2])).unwrap().is_none());
		assert!(
			group_of(&mut txn, &op, 0, 1).is_some(),
			"the row that did resolve must not have been reclaimed by a batch that failed"
		);
	}

	#[test]
	fn a_group_that_outlived_its_mapping_translates_to_nothing() {
		// The identity phase is row-budgeted, so it can take a group's mapping and run out before it
		// clears the dictionary entry - it reports `more` and leaves the group resolvable. A diff
		// arriving in that window resolves the group and finds no row number, which is the other half
		// of the all-or-nothing rule above: translating only the rows that did resolve would hand the
		// sink a Columns whose row numbers no longer line up with the values beside them.
		let engine = TestEngine::new();
		let op = op(12);
		let mut txn = txn_at(&engine, op.node, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[1, 2]))).unwrap();
		let stripped = group_of(&mut txn, &op, 0, 2).expect("precondition: both rows are interned");
		assert!(txn.remove_row_number(op.node, stripped, &AppendOperator::mapping_key()).unwrap());

		assert!(op.translate_append_update(&mut txn, 0, rows(&[1, 2]), rows(&[1, 2])).unwrap().is_none());
		assert!(op.translate_append_remove(&mut txn, 0, rows(&[1, 2])).unwrap().is_none());
		assert!(
			group_of(&mut txn, &op, 0, 1).is_some(),
			"the row that did resolve must survive a batch that could not translate"
		);
	}

	#[test]
	fn removing_a_source_row_takes_its_whole_group_with_it() {
		// Forgetting the group alone would clear the dictionary entry and the activity index but leave
		// the group record behind, and nothing can ever reach it again: the record is addressed by id,
		// the only path from bytes to id is gone, and neither reclamation index still names it. That is
		// one permanently orphaned row per removed source row, so the remove path has to run the
		// identity phase rather than just forget.
		let engine = TestEngine::new();
		let op = op(6);
		let mut txn = txn_at(&engine, op.node, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[5]))).unwrap();
		let group = group_of(&mut txn, &op, 0, 5).expect("precondition: the row is interned");
		assert!(group_rows(&mut txn, &op, group) > 0);

		op.translate_append_remove(&mut txn, 0, rows(&[5])).unwrap().expect("a known row must translate");

		assert_eq!(group_of(&mut txn, &op, 0, 5), None, "the dictionary entry must go");
		assert_eq!(group_rows(&mut txn, &op, group), 0, "and the group's range must be left empty");
	}

	#[test]
	fn an_update_restamps_activity_so_a_live_row_is_not_retired() {
		// Idleness is measured from the last stamped bucket, and an update is activity. Without the
		// restamp a row that is written every day but never re-inserted would keep the bucket of its
		// first sighting, come due while it is still being updated, and lose the mapping that names
		// its sink row - after which the next update resolves to nothing and is silently dropped.
		let engine = TestEngine::new();
		let op = op(7);
		let mut txn = txn_at(&engine, op.node, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[3]))).unwrap();
		engine.commit_pending(&mut txn);

		let mut txn = txn_at(&engine, op.node, 5 * BUCKET_WIDTH);
		let group = group_of(&mut txn, &op, 0, 3).expect("precondition: the row survived the commit");
		assert_eq!(
			txn.due_groups(op.node, Cutoff::Version(2 * BUCKET_WIDTH), 10).unwrap(),
			vec![group],
			"precondition: stamped in bucket 0, the group is due once the cutoff clears it"
		);

		op.translate_append_update(&mut txn, 0, rows(&[3]), rows(&[3]))
			.unwrap()
			.expect("a known row translates");

		assert!(
			txn.due_groups(op.node, Cutoff::Version(2 * BUCKET_WIDTH), 10).unwrap().is_empty(),
			"the update must have moved the group to the bucket it was active in"
		);
	}

	#[test]
	fn an_idle_row_loses_its_mapping_only_after_the_data_phase_released_the_group() {
		// Append holds no data at all, so its group arrives at the identity phase with an empty data
		// range. The two phases still have to run in order: the identity cutoff trails the sink row
		// ttl, and taking the mapping before that would retire the name of a sink row that is still
		// there - the update-dropping bug the operator's own ttl sweep used to cause.
		let engine = TestEngine::new();
		let op = op(8);
		let mut txn = txn_at(&engine, op.node, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[11]))).unwrap();
		let group = group_of(&mut txn, &op, 0, 11).expect("precondition: the row is interned");

		assert!(
			txn.due_identity_groups(op.node, Cutoff::Version(2 * BUCKET_WIDTH), 10).unwrap().is_empty(),
			"a group the data phase has not released is not an identity candidate"
		);

		let outcome = txn.reclaim_group_data(op.node, group, 100).unwrap();
		assert_eq!(outcome.removed, 0, "append writes no data rows, so the data phase has nothing to erase");
		txn.defer_group(op.node, group).unwrap();
		assert!(
			txn.get_row_number(op.node, group, &AppendOperator::mapping_key()).unwrap().is_some(),
			"the mapping must survive the data phase"
		);

		assert_eq!(
			txn.due_identity_groups(op.node, Cutoff::Version(2 * BUCKET_WIDTH), 10).unwrap(),
			vec![group]
		);
		txn.reclaim_group_identity(op.node, group, 100).unwrap();

		assert_eq!(group_rows(&mut txn, &op, group), 0, "the identity phase must empty the group");
		assert_eq!(group_of(&mut txn, &op, 0, 11), None, "and take the dictionary entry with it");
	}

	#[test]
	fn scheduling_no_ticks_and_declaring_no_tick_capability_move_together() {
		// fire_operator_tick returns early when ticks() is None, BEFORE enforce_tick_capability runs,
		// so the capability is only ever consulted on the path a Some interval opens. The two must
		// therefore change together in one direction only: adding a sweep back to this operator
		// without restoring the capability does not fail a check, it aborts the process the first
		// time the sweep fires. Append no longer sweeps anything - the substrate reclaims its groups -
		// so both sides are absent here, and this fails the moment only one of them comes back.
		let op = op(9);
		assert!(op.ticks().is_none(), "append schedules no operator ticks; the substrate reclaims it");
		assert!(!op.capabilities().contains(&OperatorCapability::Tick));
	}

	#[test]
	fn capabilities_declare_reclaim_or_the_substrate_skips_the_node() {
		// reclaim_flow reads the declaration, not the node type: a node that does not declare Reclaim
		// is counted perpetual and never scanned. Since append no longer evicts anything itself, losing
		// this bit turns every mapping it holds into a permanent one while the report calls it healthy.
		assert!(op(10).capabilities().contains(&OperatorCapability::Reclaim));
	}

	#[test]
	fn append_reports_no_operator_sample() {
		// Append owns no windowed operator state; its row-number mappings now live in the
		// shared row-number registry, whose telemetry is emitted by RowNumberMetricsCollector
		// (see crate::operator::metrics), not by the operator sample. So append's own sample
		// is empty - a mapping leak on an append node is attributed via the registry's
		// row_number_* metrics keyed by this node, not through a per-operator sample here.
		assert!(op(11).sample().is_none(), "append has no owned operator state to sample");
	}
}
