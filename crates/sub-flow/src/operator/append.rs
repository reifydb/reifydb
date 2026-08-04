// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	key::operator_group_state::{GroupId, GroupSet},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::{Operator, Reclaimable},
	transaction::FlowTransaction,
};
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::instrument;

use crate::{
	error::FlowGraphError,
	operator::{OperatorCell, drops::SealedDrops},
};

const CAPABILITIES: &[OperatorCapability] = &[
	OperatorCapability::Insert,
	OperatorCapability::Update,
	OperatorCapability::Delete,
	OperatorCapability::Reclaim,
];

const REMOVE_RECLAIM_LIMIT: usize = 8;

const DROP_REASON: &str = "mutations whose source row mapping was reclaimed";

pub struct AppendOperator {
	operator: OperatorId,

	parents: Vec<OperatorCell>,

	input_nodes: Vec<OperatorId>,

	dropped: SealedDrops,

	ttl: Option<Duration>,
}

impl AppendOperator {
	pub fn new(
		operator: OperatorId,
		parents: Vec<OperatorCell>,
		input_nodes: Vec<OperatorId>,
		ttl: Option<Duration>,
	) -> Self {
		reifydb_assertions! {
			assert_eq!(parents.len(), input_nodes.len());
			assert!(parents.len() >= 2, "Append requires at least 2 inputs");
		}

		Self {
			operator,
			parents,
			input_nodes,
			dropped: SealedDrops::new(operator, DROP_REASON),
			ttl,
		}
	}

	#[cfg(test)]
	pub(crate) fn new_for_state_tests(operator: OperatorId) -> Self {
		Self {
			operator,
			parents: Vec::new(),
			input_nodes: Vec::new(),
			dropped: SealedDrops::new(operator, DROP_REASON),
			ttl: None,
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
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn retention_scale(&self) -> Option<Duration> {
		self.ttl
	}

	fn reclaimable_through(&self, _txn: &mut FlowTransaction, watermark: DateTime) -> Result<Reclaimable> {
		Ok(self.ttl.map(|ttl| Reclaimable::data(watermark.saturating_sub(ttl))).unwrap_or_default())
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

		Ok(Change::from_flow(self.operator, change.version, result_diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

impl AppendOperator {
	#[inline]
	#[instrument(name = "flow::operator::append::create_row_numbers", level = "trace", skip_all, fields(groups = groups.len()))]
	fn translate_create_row_numbers(
		&self,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Vec<RowNumber>> {
		let interned = txn.intern_groups(self.operator, groups)?;
		let mut output_row_numbers = Vec::with_capacity(interned.len());
		for (group, _) in interned {
			let (output_row_number, _) =
				txn.get_or_create_row_number(self.operator, group, &Self::mapping_key())?;
			output_row_numbers.push(output_row_number);
		}
		Ok(output_row_numbers)
	}

	#[inline]
	#[instrument(name = "flow::operator::append::lookup_row_numbers", level = "trace", skip_all, fields(groups = groups.len()))]
	fn lookup_row_numbers(
		&self,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Option<(Vec<RowNumber>, Vec<GroupId>)>> {
		let mut output_row_numbers = Vec::with_capacity(groups.len());
		let mut ids = Vec::with_capacity(groups.len());
		for group_bytes in groups {
			let Some(group) = txn.lookup_group(self.operator, group_bytes)? else {
				return Ok(None);
			};
			let Some(row_number) = txn.get_row_number(self.operator, group, &Self::mapping_key())? else {
				return Ok(None);
			};
			output_row_numbers.push(row_number);
			ids.push(group);
		}
		Ok(Some((output_row_numbers, ids)))
	}

	#[inline]
	#[instrument(name = "flow::operator::append::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
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
	#[instrument(name = "flow::operator::append::update", level = "trace", skip_all, fields(rows = post.row_count()))]
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
			self.dropped.note(post.row_count() as u64);
			return Ok(None);
		};
		txn.intern_groups(self.operator, &groups)?;
		let pre_output = pre.with_row_numbers(output_row_numbers.clone());
		let post_output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::update(pre_output, post_output)))
	}

	#[inline]
	#[instrument(name = "flow::operator::append::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
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
			self.dropped.note(pre.row_count() as u64);
			return Ok(None);
		};
		for group in &ids {
			txn.reclaim_group_identity(self.operator, *group, REMOVE_RECLAIM_LIMIT)?;
		}
		txn.invalidate_row_number_groups(self.operator, &GroupSet::new(ids));
		let output = pre.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::remove(output)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion, key::operator_group_state::group_inner_range, state::horizon::Cutoff,
		value::column::columns::Columns,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_flow::transaction::ChangeCoordinate;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::{
		count::Count,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::*;

	const BUCKET_WIDTH: u64 = 3_750_000_000;

	fn op(operator: u64) -> AppendOperator {
		AppendOperator::new_for_state_tests(OperatorId(operator))
	}

	fn txn_at(engine: &TestEngine, operator: OperatorId, coordinate: u64) -> FlowTransaction {
		// Registering the horizon mirrors what register.rs does in production; without it the
		// operator falls back to the interner's default bucket width and stamps in no domain.
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.group_interner().set_activity_grid(operator, Some(Duration::from_seconds(60).unwrap()));
		txn.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_nanos(coordinate),
			version: CommitVersion(coordinate),
		});
		txn
	}

	fn rows(source_rows: &[u64]) -> Columns {
		Columns::empty().with_row_numbers(source_rows.iter().map(|r| RowNumber(*r)).collect())
	}

	fn group_of(txn: &mut FlowTransaction, op: &AppendOperator, parent: u8, source_row: u64) -> Option<GroupId> {
		txn.lookup_group(op.operator, &AppendOperator::group_bytes(parent, RowNumber(source_row))).unwrap()
	}

	fn group_rows(txn: &mut FlowTransaction, op: &AppendOperator, group: GroupId) -> usize {
		txn.state_range(op.operator, group_inner_range(group), None, "test").unwrap().items.len()
	}

	#[test]
	fn a_source_row_interns_a_group_that_carries_its_output_row_number() {
		// The mapping lives at the group's own address, which is what puts it inside the range the
		// identity phase deletes; written anywhere else it would be invisible to reclamation and
		// leak one row per source row for the life of the operator.
		let engine = TestEngine::new();
		let op = op(1);
		let mut txn = txn_at(&engine, op.operator, 100);

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
			txn.get_row_number(op.operator, group, &AppendOperator::mapping_key()).unwrap(),
			Some(post.row_numbers()[0]),
			"the output row number must be readable from inside the group that owns it"
		);
	}

	#[test]
	fn the_same_source_row_always_translates_to_the_same_output_row() {
		// A source row keeps one identity downstream for as long as the mapping lives; a second
		// insert that minted a fresh number would duplicate the sink row rather than replace it.
		let engine = TestEngine::new();
		let op = op(2);
		let mut txn = txn_at(&engine, op.operator, 100);

		let first =
			op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[7]))).unwrap();
		let second =
			op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[7]))).unwrap();

		assert_eq!(first, second, "an already-interned source row must resolve to its existing output row");
	}

	#[test]
	fn each_input_numbers_its_own_source_rows_independently() {
		// A union's inputs number their rows independently, so the parent index has to be in the
		// group bytes: sharing one group would collapse two unrelated source rows onto a single
		// output row and let either input's reclamation erase the other's mapping.
		let engine = TestEngine::new();
		let op = op(3);
		let mut txn = txn_at(&engine, op.operator, 100);

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
		// If lookup interned, every unmatched diff would leave a dictionary entry, a group record
		// and an activity-index row addressing a mapping that does not exist - unbounded growth
		// driven entirely by traffic the operator drops on the floor.
		let engine = TestEngine::new();
		let op = op(4);
		let mut txn = txn_at(&engine, op.operator, 100);

		assert!(op.translate_append_update(&mut txn, 0, rows(&[99]), rows(&[99])).unwrap().is_none());
		assert!(op.translate_append_remove(&mut txn, 0, rows(&[99])).unwrap().is_none());

		assert_eq!(group_of(&mut txn, &op, 0, 99), None, "a lookup must not have interned the missing row");
	}

	#[test]
	fn a_partly_known_batch_translates_to_nothing_at_all() {
		// The diff carries one Columns for the whole batch, so it is all-or-nothing: emitting only
		// the rows that resolved hands the sink row numbers that no longer line up with the values.
		let engine = TestEngine::new();
		let op = op(5);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[1]))).unwrap();

		assert!(op.translate_append_remove(&mut txn, 0, rows(&[1, 2])).unwrap().is_none());
		assert!(
			group_of(&mut txn, &op, 0, 1).is_some(),
			"the row that did resolve must not have been reclaimed by a batch that failed"
		);
	}

	#[test]
	fn a_group_that_outlived_its_mapping_translates_to_nothing() {
		// The identity phase is row-budgeted, so it can take the mapping and run out before it
		// clears the dictionary entry. A diff arriving in that window resolves the group and finds
		// no row number, which is the other half of the all-or-nothing rule.
		let engine = TestEngine::new();
		let op = op(12);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[1, 2]))).unwrap();
		let stripped = group_of(&mut txn, &op, 0, 2).expect("precondition: both rows are interned");
		assert!(txn.remove_row_number(op.operator, stripped, &AppendOperator::mapping_key()).unwrap());

		assert!(op.translate_append_update(&mut txn, 0, rows(&[1, 2]), rows(&[1, 2])).unwrap().is_none());
		assert!(op.translate_append_remove(&mut txn, 0, rows(&[1, 2])).unwrap().is_none());
		assert!(
			group_of(&mut txn, &op, 0, 1).is_some(),
			"the row that did resolve must survive a batch that could not translate"
		);
	}

	#[test]
	fn every_untranslatable_mutation_is_counted_rather_than_swallowed() {
		// A dropped mutation leaves the sink holding a row that is never updated or withdrawn
		// again, and this counter is the only evidence. Counting per call rather than per row
		// would under-report the leak by the batch size.
		let engine = TestEngine::new();
		let op = op(13);
		let mut txn = txn_at(&engine, op.operator, 100);
		assert_eq!(op.dropped.total(), 0, "nothing has been dropped yet");

		assert!(op.translate_append_remove(&mut txn, 0, rows(&[99])).unwrap().is_none());
		assert_eq!(op.dropped.total(), 1, "a remove for an unknown row discards that row");

		assert!(op
			.translate_append_update(&mut txn, 0, rows(&[1, 2, 3, 4]), rows(&[1, 2, 3, 4]))
			.unwrap()
			.is_none());
		assert_eq!(op.dropped.total(), 5, "an update for four unknown rows discards four more");

		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[7]))).unwrap();
		op.translate_append_remove(&mut txn, 0, rows(&[7])).unwrap().expect("a known row must translate");
		assert_eq!(op.dropped.total(), 5, "a mutation that did translate must not be counted as a drop");
	}

	#[test]
	fn removing_a_source_row_takes_its_whole_group_with_it() {
		// Forgetting the group alone leaves the group record behind with no path from bytes to id
		// and no index naming it - one permanently orphaned row per removed source row - so the
		// remove path has to run the identity phase.
		let engine = TestEngine::new();
		let op = op(6);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[5]))).unwrap();
		let group = group_of(&mut txn, &op, 0, 5).expect("precondition: the row is interned");
		assert!(group_rows(&mut txn, &op, group) > 0);

		op.translate_append_remove(&mut txn, 0, rows(&[5])).unwrap().expect("a known row must translate");

		assert_eq!(group_of(&mut txn, &op, 0, 5), None, "the dictionary entry must go");
		assert_eq!(group_rows(&mut txn, &op, group), 0, "and the group's range must be left empty");
	}

	#[test]
	fn an_update_restamps_activity_so_a_live_row_is_not_retired() {
		// Idleness is measured from the last stamped bucket, so without a restamp a row updated
		// daily but never re-inserted comes due on the bucket of its first sighting and loses the
		// mapping naming its sink row; the next update then resolves to nothing.
		let engine = TestEngine::new();
		let op = op(7);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[3]))).unwrap();
		engine.commit_pending(&mut txn);

		let mut txn = txn_at(&engine, op.operator, 5 * BUCKET_WIDTH);
		let group = group_of(&mut txn, &op, 0, 3).expect("precondition: the row survived the commit");
		assert_eq!(
			txn.due_groups(op.operator, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10).unwrap(),
			vec![group],
			"precondition: stamped in bucket 0, the group is due once the cutoff clears it"
		);

		op.translate_append_update(&mut txn, 0, rows(&[3]), rows(&[3]))
			.unwrap()
			.expect("a known row translates");

		assert!(
			txn.due_groups(op.operator, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10)
				.unwrap()
				.is_empty(),
			"the update must have moved the group to the bucket it was active in"
		);
	}

	#[test]
	fn removing_a_source_row_also_takes_its_mapping_out_of_the_row_number_cache() {
		// Append reclaims identity itself rather than waiting for the sweep that would tell the
		// provider, so erasing the rows under a live cache leaves one unreachable entry per
		// removed source row: the group id is never reissued, and nothing reads or evicts it.
		let engine = TestEngine::new();
		let op = op(14);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[1, 2]))).unwrap();
		let provider = txn.row_numbers();
		assert_eq!(
			provider.memory(op.operator).entries,
			Count::new(2),
			"precondition: both mappings are cached"
		);

		op.translate_append_remove(&mut txn, 0, rows(&[1])).unwrap().expect("a known row must translate");

		assert_eq!(
			provider.memory(op.operator).entries,
			Count::new(1),
			"the removed row's mapping must leave the cache with the rows it named"
		);
	}

	#[test]
	fn an_idle_row_loses_its_mapping_only_after_the_data_phase_released_the_group() {
		// Append holds no data, so its group reaches the identity phase with an empty data range.
		// The order still matters: the identity cutoff trails the sink row ttl, and taking the
		// mapping earlier retires the name of a sink row that is still there.
		let engine = TestEngine::new();
		let op = op(8);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut txn, &AppendOperator::group_keys(0, &rows(&[11]))).unwrap();
		let group = group_of(&mut txn, &op, 0, 11).expect("precondition: the row is interned");

		assert!(
			txn.due_identity_groups(op.operator, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10)
				.unwrap()
				.is_empty(),
			"a group the data phase has not released is not an identity candidate"
		);

		let outcome = txn.reclaim_group_data(op.operator, group, 100).unwrap();
		assert_eq!(outcome.removed, 0, "append writes no data rows, so the data phase has nothing to erase");
		txn.defer_group(op.operator, group).unwrap();
		assert!(
			txn.get_row_number(op.operator, group, &AppendOperator::mapping_key()).unwrap().is_some(),
			"the mapping must survive the data phase"
		);

		assert_eq!(
			txn.due_identity_groups(op.operator, Cutoff(DateTime::from_nanos(2 * BUCKET_WIDTH)), 10)
				.unwrap(),
			vec![group]
		);
		txn.reclaim_group_identity(op.operator, group, 100).unwrap();

		assert_eq!(group_rows(&mut txn, &op, group), 0, "the identity phase must empty the group");
		assert_eq!(group_of(&mut txn, &op, 0, 11), None, "and take the dictionary entry with it");
	}

	#[test]
	fn capabilities_declare_reclaim_or_the_substrate_skips_the_node() {
		// The sweep reads the declaration, not the operator type, so a operator that omits Reclaim is
		// counted perpetual and never scanned - every mapping it holds becomes permanent while
		// the report calls it healthy.
		assert!(op(10).capabilities().contains(&OperatorCapability::Reclaim));
	}

	#[test]
	fn append_reports_no_operator_sample() {
		// Append's mappings live in the shared row-number registry, so a mapping leak here is
		// attributed through the registry's per-operator metrics, not a per-operator sample.
		assert!(op(11).sample().is_none(), "append has no owned operator state to sample");
	}
}
