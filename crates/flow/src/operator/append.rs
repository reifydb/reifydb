// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::{encoded::EncodedKey, serializer::KeySerializer},
	row::operator::{OperatorState, decode},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	key::operator_state::{GroupId, GroupSet, GroupStateKey, Keyspace, OperatorStateKey},
	metrics::heap::OperatorSample,
	state::store::TimerKind,
	value::column::columns::Columns,
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::instrument;

use crate::{
	error::FlowGraphError,
	operator::{HostOperator, drops::SealedDrops, host::HostContext},
	state::{
		expiry::{ExpiryIndex, expiry_key},
		reaper::{StoreReaper, drain, enqueue, queue_key, queued},
		seal::{coord::Coord, ledger::FiredAt, policy::SealPolicy},
	},
	timer::Timer,
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

const REMOVE_RECLAIM_LIMIT: usize = 8;

const SEAL_BATCH: usize = 256;

const DROP_REASON: &str = "mutations whose source row mapping was reclaimed";

#[operator_state]
#[derive(Clone)]
pub struct SealEntry {
	group_id: u64,
}

#[operator_state]
#[derive(Clone)]
pub struct SealAnchor {
	expiry: DateTime,
}

pub struct AppendOperator {
	operator: OperatorId,

	parent_schema: Option<Columns>,

	input_nodes: Vec<OperatorId>,

	dropped: SealedDrops,

	seal: Option<Duration>,

	expiry: ExpiryIndex<SealEntry>,
}

impl AppendOperator {
	pub fn new(
		operator: OperatorId,
		parent_schema: Option<Columns>,
		input_nodes: Vec<OperatorId>,
		seal: Option<Duration>,
	) -> Self {
		reifydb_assertions! {
			assert!(input_nodes.len() >= 2, "Append requires at least 2 inputs");
		}

		Self {
			operator,
			parent_schema,
			input_nodes,
			dropped: SealedDrops::new(operator, DROP_REASON),
			seal: seal.filter(|span| !span.is_zero()),
			expiry: ExpiryIndex::new(),
		}
	}

	#[cfg(test)]
	pub(crate) fn new_for_state_tests(operator: OperatorId) -> Self {
		Self {
			operator,
			parent_schema: None,
			input_nodes: Vec::new(),
			dropped: SealedDrops::new(operator, DROP_REASON),
			seal: None,
			expiry: ExpiryIndex::new(),
		}
	}

	#[cfg(test)]
	pub(crate) fn sealing_for_state_tests(operator: OperatorId, seal: Duration) -> Self {
		Self {
			seal: Some(seal),
			..Self::new_for_state_tests(operator)
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent_schema.clone()
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

	fn anchor_key(group: GroupId) -> GroupStateKey {
		OperatorStateKey::inner_encoded(group, Keyspace::CUSTOM, Vec::new())
	}

	fn read_anchor(host: &mut dyn HostContext, group: GroupId) -> Result<Option<DateTime>> {
		let Some(bytes) = host.state_get(&Self::anchor_key(group))? else {
			return Ok(None);
		};
		Ok(Some(decode::<SealAnchor>(&bytes)?.expiry))
	}

	fn arm_seal(&mut self, host: &mut dyn HostContext, groups: &[EncodedKey], columns: &Columns) -> Result<()> {
		let Some(seal) = self.seal else {
			return Ok(());
		};
		let policy = SealPolicy::of(seal);
		let times = columns.time().to_vec();
		for (index, resolved) in host.lookup_groups(groups)?.into_iter().enumerate() {
			let (Some(group), Some(at)) = (resolved, times.get(index)) else {
				continue;
			};
			self.move_anchor(host, group, policy.seal_instant(*at).at())?;
		}
		self.arm_maintenance(host, None)
	}

	fn move_anchor(&mut self, host: &mut dyn HostContext, group: GroupId, expiry: DateTime) -> Result<()> {
		let prior = Self::read_anchor(host, group)?;
		if prior == Some(expiry) {
			return Ok(());
		}
		if let Some(prior) = prior {
			self.expiry.drop_key(host, &expiry_key(prior.to_order(), &group.0, &[]))?;
			host.state_remove(&queue_key(group))?;
		}
		self.expiry.set(
			host,
			expiry_key(expiry.to_order(), &group.0, &[]),
			SealEntry {
				group_id: group.0,
			},
		)?;
		let written_at = host.written_at();
		host.state_set(
			&Self::anchor_key(group),
			SealAnchor {
				expiry,
			}
			.encode_state(written_at)?,
		)
	}

	fn clear_seal(&mut self, host: &mut dyn HostContext, groups: &[GroupId]) -> Result<()> {
		if self.seal.is_none() {
			return Ok(());
		}
		for group in groups {
			let Some(expiry) = Self::read_anchor(host, *group)? else {
				continue;
			};
			self.expiry.drop_key(host, &expiry_key(expiry.to_order(), &group.0, &[]))?;
			host.state_remove(&Self::anchor_key(*group))?;
		}
		Ok(())
	}

	fn arm_maintenance(&mut self, host: &mut dyn HostContext, retry: Option<DateTime>) -> Result<()> {
		let earliest = self.expiry.earliest(host)?.map(<DateTime as Coord>::from_order);
		let Some(at) = earliest.into_iter().chain(retry).min() else {
			return Ok(());
		};
		host.arm_timer(at, TimerKind::Maintenance, &EncodedKey::new(Vec::new()))
	}

	fn seal_due_rows(&mut self, host: &mut dyn HostContext, fired: FiredAt) -> Result<usize> {
		let Some(seal) = self.seal else {
			return Ok(0);
		};
		for (key, entry) in self.expiry.due(host, fired.at().to_order(), SEAL_BATCH)? {
			enqueue(host, GroupId(entry.group_id))?;
			self.expiry.drop_key(host, &key)?;
		}
		let freed = drain(host, &mut StoreReaper, SEAL_BATCH)?;
		let retry = (!queued(host, 1)?.is_empty()).then(|| fired.at().saturating_add(seal));
		self.arm_maintenance(host, retry)?;
		Ok(freed)
	}
}

impl HostOperator for AppendOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
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
					if let Some(d) = self.translate_append_insert(host, parent_index, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					if let Some(d) = self.translate_append_update(host, parent_index, pre, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					if let Some(d) = self.translate_append_remove(host, parent_index, pre)? {
						result_diffs.push(d);
					}
				}
			}
		}

		Ok(Change::from_flow(self.operator, change.version, result_diffs, change.changed_at))
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		if timer.kind == TimerKind::Maintenance {
			self.seal_due_rows(host, FiredAt::of(&timer))?;
		}
		Ok(None)
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
		host: &mut dyn HostContext,
		groups: &[EncodedKey],
	) -> Result<Vec<RowNumber>> {
		let interned = host.intern_groups(groups)?;
		let mut output_row_numbers = Vec::with_capacity(interned.len());
		for (group, _) in interned {
			let (output_row_number, _) = host.get_or_create_row_number(group, &Self::mapping_key())?;
			output_row_numbers.push(output_row_number);
		}
		Ok(output_row_numbers)
	}

	#[inline]
	#[instrument(name = "flow::operator::append::lookup_row_numbers", level = "trace", skip_all, fields(groups = groups.len()))]
	fn lookup_row_numbers(
		&self,
		host: &mut dyn HostContext,
		groups: &[EncodedKey],
	) -> Result<Option<(Vec<RowNumber>, Vec<GroupId>)>> {
		let mut output_row_numbers = Vec::with_capacity(groups.len());
		let mut ids = Vec::with_capacity(groups.len());
		for group_bytes in groups {
			let Some(group) = host.lookup_group(group_bytes)? else {
				return Ok(None);
			};
			let Some(row_number) = host.get_row_number(group, &Self::mapping_key())? else {
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
		&mut self,
		host: &mut dyn HostContext,
		parent_index: usize,
		post: Columns,
	) -> Result<Option<Diff>> {
		if post.row_count() == 0 {
			return Ok(None);
		}
		let groups = Self::group_keys(parent_index, &post);
		let output_row_numbers = self.translate_create_row_numbers(host, &groups)?;
		self.arm_seal(host, &groups, &post)?;
		let output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::insert(output)))
	}

	#[inline]
	#[instrument(name = "flow::operator::append::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn translate_append_update(
		&mut self,
		host: &mut dyn HostContext,
		parent_index: usize,
		pre: Columns,
		post: Columns,
	) -> Result<Option<Diff>> {
		if post.row_count() == 0 {
			return Ok(None);
		}
		let groups = Self::group_keys(parent_index, &pre);
		let Some((output_row_numbers, _)) = self.lookup_row_numbers(host, &groups)? else {
			self.dropped.note(post.row_count() as u64);
			return Ok(None);
		};
		host.intern_groups(&groups)?;
		self.arm_seal(host, &groups, &post)?;
		let pre_output = pre.with_row_numbers(output_row_numbers.clone());
		let post_output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::update(pre_output, post_output)))
	}

	#[inline]
	#[instrument(name = "flow::operator::append::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn translate_append_remove(
		&mut self,
		host: &mut dyn HostContext,
		parent_index: usize,
		pre: Columns,
	) -> Result<Option<Diff>> {
		if pre.row_count() == 0 {
			return Ok(None);
		}
		let groups = Self::group_keys(parent_index, &pre);
		let Some((output_row_numbers, ids)) = self.lookup_row_numbers(host, &groups)? else {
			self.dropped.note(pre.row_count() as u64);
			return Ok(None);
		};
		self.clear_seal(host, &ids)?;
		for group in &ids {
			host.reclaim_group_identity(*group, REMOVE_RECLAIM_LIMIT)?;
		}
		host.invalidate_row_number_groups(&GroupSet::new(ids));
		let output = pre.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::remove(output)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion, key::operator_state::group_inner_range, value::column::columns::Columns,
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{count::Count, factory::time::at_millis, value::datetime::DateTime};

	use super::*;
	use crate::{
		operator::host::TxnHostContext,
		state::expiry::expiry_range,
		testing::FlowTxn,
		transaction::{
			ChangeCoordinate, FlowTransaction, deferred::DeferredTransaction, group::GroupTxn,
			row_number::RowNumberTxn, state::StateTxn,
		},
	};

	fn op(operator: u64) -> AppendOperator {
		AppendOperator::new_for_state_tests(OperatorId(operator))
	}

	fn host<'a>(txn: &'a mut DeferredTransaction, op: &AppendOperator) -> TxnHostContext<'a, DeferredTransaction> {
		TxnHostContext::new(txn, op.operator)
	}

	fn txn_at(engine: &TestEngine, _operator: OperatorId, coordinate: u64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_nanos(coordinate)),
			version: CommitVersion(coordinate),
		});
		txn
	}

	fn rows(source_rows: &[u64]) -> Columns {
		Columns::empty().with_row_numbers(source_rows.iter().map(|r| RowNumber(*r)).collect())
	}

	fn timed(source_rows: &[u64], at: DateTime) -> Columns {
		let mut columns = rows(source_rows);
		columns.system.set_time(vec![at; source_rows.len()]);
		columns
	}

	fn sealing(operator: u64) -> AppendOperator {
		AppendOperator::sealing_for_state_tests(OperatorId(operator), Duration::from_seconds(10).unwrap())
	}

	fn fire(op: &mut AppendOperator, txn: &mut DeferredTransaction, at: DateTime) {
		let operator = op.operator;
		op.on_timer(
			&mut TxnHostContext::new(txn, operator),
			Timer {
				at,
				kind: TimerKind::Maintenance,
				key: EncodedKey::new(Vec::new()),
			},
		)
		.unwrap();
	}

	fn index_entries(txn: &mut DeferredTransaction, op: &AppendOperator) -> usize {
		txn.state_range(op.operator, expiry_range(), None, "test").unwrap().items.len()
	}

	fn anchor_of(txn: &mut DeferredTransaction, op: &AppendOperator, group: GroupId) -> Option<DateTime> {
		AppendOperator::read_anchor(&mut host(txn, op), group).unwrap()
	}

	fn group_of(
		txn: &mut DeferredTransaction,
		op: &AppendOperator,
		parent: u8,
		source_row: u64,
	) -> Option<GroupId> {
		txn.lookup_group(op.operator, &AppendOperator::group_bytes(parent, RowNumber(source_row))).unwrap()
	}

	fn group_rows(txn: &mut DeferredTransaction, op: &AppendOperator, group: GroupId) -> usize {
		txn.state_range(op.operator, group_inner_range(group), None, "test").unwrap().items.len()
	}

	#[test]
	fn a_source_row_interns_a_group_that_carries_its_output_row_number() {
		// The mapping lives at the group's own address, which is what puts it inside the range the
		// identity phase deletes; written anywhere else it would be invisible to reclamation and
		// leak one row per source row for the life of the operator.
		let engine = TestEngine::new();
		let mut op = op(1);
		let mut txn = txn_at(&engine, op.operator, 100);

		let assigned = op.translate_append_insert(&mut host(&mut txn, &op), 0, rows(&[42])).unwrap().unwrap();
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

		let first = op
			.translate_create_row_numbers(
				&mut host(&mut txn, &op),
				&AppendOperator::group_keys(0, &rows(&[7])),
			)
			.unwrap();
		let second = op
			.translate_create_row_numbers(
				&mut host(&mut txn, &op),
				&AppendOperator::group_keys(0, &rows(&[7])),
			)
			.unwrap();

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

		let left = op
			.translate_create_row_numbers(
				&mut host(&mut txn, &op),
				&AppendOperator::group_keys(0, &rows(&[7])),
			)
			.unwrap();
		let right = op
			.translate_create_row_numbers(
				&mut host(&mut txn, &op),
				&AppendOperator::group_keys(1, &rows(&[7])),
			)
			.unwrap();

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
		let mut op = op(4);
		let mut txn = txn_at(&engine, op.operator, 100);

		assert!(op
			.translate_append_update(&mut host(&mut txn, &op), 0, rows(&[99]), rows(&[99]))
			.unwrap()
			.is_none());
		assert!(op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[99])).unwrap().is_none());

		assert_eq!(group_of(&mut txn, &op, 0, 99), None, "a lookup must not have interned the missing row");
	}

	#[test]
	fn a_partly_known_batch_translates_to_nothing_at_all() {
		// The diff carries one Columns for the whole batch, so it is all-or-nothing: emitting only
		// the rows that resolved hands the sink row numbers that no longer line up with the values.
		let engine = TestEngine::new();
		let mut op = op(5);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut host(&mut txn, &op), &AppendOperator::group_keys(0, &rows(&[1])))
			.unwrap();

		assert!(op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[1, 2])).unwrap().is_none());
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
		let mut op = op(12);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(
			&mut host(&mut txn, &op),
			&AppendOperator::group_keys(0, &rows(&[1, 2])),
		)
		.unwrap();
		let stripped = group_of(&mut txn, &op, 0, 2).expect("precondition: both rows are interned");
		assert!(txn.remove_row_number(op.operator, stripped, &AppendOperator::mapping_key()).unwrap());

		assert!(op
			.translate_append_update(&mut host(&mut txn, &op), 0, rows(&[1, 2]), rows(&[1, 2]))
			.unwrap()
			.is_none());
		assert!(op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[1, 2])).unwrap().is_none());
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
		let mut op = op(13);
		let mut txn = txn_at(&engine, op.operator, 100);
		assert_eq!(op.dropped.total(), 0, "nothing has been dropped yet");

		assert!(op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[99])).unwrap().is_none());
		assert_eq!(op.dropped.total(), 1, "a remove for an unknown row discards that row");

		assert!(op
			.translate_append_update(&mut host(&mut txn, &op), 0, rows(&[1, 2, 3, 4]), rows(&[1, 2, 3, 4]))
			.unwrap()
			.is_none());
		assert_eq!(op.dropped.total(), 5, "an update for four unknown rows discards four more");

		op.translate_create_row_numbers(&mut host(&mut txn, &op), &AppendOperator::group_keys(0, &rows(&[7])))
			.unwrap();
		op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[7]))
			.unwrap()
			.expect("a known row must translate");
		assert_eq!(op.dropped.total(), 5, "a mutation that did translate must not be counted as a drop");
	}

	#[test]
	fn removing_a_source_row_takes_its_whole_group_with_it() {
		// Forgetting the group alone leaves the group record behind with no path from bytes to id
		// and no index naming it - one permanently orphaned row per removed source row - so the
		// remove path has to run the identity phase.
		let engine = TestEngine::new();
		let mut op = op(6);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut host(&mut txn, &op), &AppendOperator::group_keys(0, &rows(&[5])))
			.unwrap();
		let group = group_of(&mut txn, &op, 0, 5).expect("precondition: the row is interned");
		assert!(group_rows(&mut txn, &op, group) > 0);

		op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[5]))
			.unwrap()
			.expect("a known row must translate");

		assert_eq!(group_of(&mut txn, &op, 0, 5), None, "the dictionary entry must go");
		assert_eq!(group_rows(&mut txn, &op, group), 0, "and the group's range must be left empty");
	}

	#[test]
	fn removing_a_source_row_also_takes_its_mapping_out_of_the_row_number_cache() {
		// Append reclaims identity itself rather than waiting for the sweep that would tell the
		// provider, so erasing the rows under a live cache leaves one unreachable entry per
		// removed source row: the group id is never reissued, and nothing reads or evicts it.
		let engine = TestEngine::new();
		let mut op = op(14);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(
			&mut host(&mut txn, &op),
			&AppendOperator::group_keys(0, &rows(&[1, 2])),
		)
		.unwrap();
		let provider = txn.row_numbers();
		assert_eq!(
			provider.memory(op.operator).entries,
			Count::new(2),
			"precondition: both mappings are cached"
		);

		op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[1]))
			.unwrap()
			.expect("a known row must translate");

		assert_eq!(
			provider.memory(op.operator).entries,
			Count::new(1),
			"the removed row's mapping must leave the cache with the rows it named"
		);
	}

	#[test]
	fn an_inserted_row_is_indexed_one_seal_past_its_own_event_time() {
		// The anchor must be the row's own event time; a wall-clock seal evicts a backfilled row on arrival.
		let engine = TestEngine::new();
		let mut op = sealing(20);
		let mut txn = txn_at(&engine, op.operator, 100);

		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		let group = group_of(&mut txn, &op, 0, 42).expect("the row must have interned a group");
		assert_eq!(
			anchor_of(&mut txn, &op, group),
			Some(at_millis(15_001)),
			"the due time is event time + seal + the strict gate step"
		);
		assert_eq!(index_entries(&mut txn, &op), 1, "and exactly one index entry addresses that row");
	}

	#[test]
	fn an_update_moves_the_rows_index_entry_rather_than_adding_a_second() {
		// Without dropping the old entry the row is addressed twice and the stale one comes due while it lives.
		let engine = TestEngine::new();
		let mut op = sealing(21);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[7], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		op.translate_append_update(
			&mut host(&mut txn, &op),
			0,
			timed(&[7], at_millis(5_000)),
			timed(&[7], at_millis(20_000)),
		)
		.unwrap()
		.expect("a known row must translate");

		let group = group_of(&mut txn, &op, 0, 7).expect("the row is interned");
		assert_eq!(index_entries(&mut txn, &op), 1, "an update re-arms one entry, it does not add one");
		assert_eq!(
			anchor_of(&mut txn, &op, group),
			Some(at_millis(30_001)),
			"and the due time follows the row"
		);
	}

	#[test]
	fn a_sealed_row_loses_its_dictionary_entry_its_group_and_its_mapping() {
		// Identity is the only state append owns, so a seal that frees none of it leaks a group per source row.
		let engine = TestEngine::new();
		let mut op = sealing(22);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		let group = group_of(&mut txn, &op, 0, 42).expect("precondition: the row is interned");

		fire(&mut op, &mut txn, at_millis(15_001));

		assert_eq!(group_of(&mut txn, &op, 0, 42), None, "the dictionary entry must go");
		assert_eq!(group_rows(&mut txn, &op, group), 0, "the group's range must be left empty");
		assert_eq!(index_entries(&mut txn, &op), 0, "and the index entry that drove the seal must drain");
	}

	#[test]
	fn a_row_still_inside_its_seal_survives_a_maintenance_tick() {
		// The gate is strict: a row whose due time lands exactly on the tick must not seal yet.
		let engine = TestEngine::new();
		let mut op = sealing(23);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		fire(&mut op, &mut txn, at_millis(15_000));

		let group = group_of(&mut txn, &op, 0, 42).expect("a row one millisecond short of its seal must live");
		assert!(group_rows(&mut txn, &op, group) > 0, "and must keep the state that resolves it");
		assert_eq!(index_entries(&mut txn, &op), 1, "its index entry is not due and must not have drained");
	}

	#[test]
	fn a_mutation_arriving_after_the_seal_is_counted_rather_than_translated() {
		// A sealed row's published row is frozen, so the discarded mutation must be counted or it vanishes
		// silently.
		let engine = TestEngine::new();
		let mut op = sealing(24);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		fire(&mut op, &mut txn, at_millis(15_001));

		let translated = op
			.translate_append_update(
				&mut host(&mut txn, &op),
				0,
				timed(&[42], at_millis(20_000)),
				timed(&[42], at_millis(20_000)),
			)
			.unwrap();

		assert!(translated.is_none(), "a row whose mapping was reclaimed cannot be updated in place");
		assert_eq!(op.dropped.total(), 1, "and the discarded mutation must be counted");
	}

	#[test]
	fn removing_a_row_takes_its_index_entry_and_its_anchor_with_it() {
		// The anchor sits in the group's data range, which the inline identity reclaim never touches.
		let engine = TestEngine::new();
		let mut op = sealing(25);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[5], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		let group = group_of(&mut txn, &op, 0, 5).expect("precondition: the row is interned");

		op.translate_append_remove(&mut host(&mut txn, &op), 0, timed(&[5], at_millis(5_000)))
			.unwrap()
			.expect("a known row must translate");

		assert_eq!(index_entries(&mut txn, &op), 0, "the index entry must go with the row");
		assert_eq!(anchor_of(&mut txn, &op, group), None, "and so must the anchor behind it");
	}

	#[test]
	fn an_updated_row_leaves_the_reap_queue_it_was_already_placed_in() {
		// A row re-armed while queued must leave the queue, or the next tick reaps identity that is live.
		let engine = TestEngine::new();
		let mut op = sealing(26);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[7], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		let group = group_of(&mut txn, &op, 0, 7).expect("precondition: the row is interned");
		enqueue(&mut host(&mut txn, &op), group).unwrap();

		op.translate_append_update(
			&mut host(&mut txn, &op),
			0,
			timed(&[7], at_millis(5_000)),
			timed(&[7], at_millis(20_000)),
		)
		.unwrap()
		.expect("a known row must translate");

		assert!(
			queued(&mut host(&mut txn, &op), 16).unwrap().is_empty(),
			"a re-armed row must not be left waiting in the reap queue"
		);
	}

	#[test]
	fn an_operator_without_a_seal_indexes_nothing_at_all() {
		// Indexing without a seal leaves one entry and one anchor per row that nothing ever collects.
		let engine = TestEngine::new();
		let mut op = op(27);
		let mut txn = txn_at(&engine, op.operator, 100);

		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		let group = group_of(&mut txn, &op, 0, 42).expect("the row must still intern a group");
		assert_eq!(index_entries(&mut txn, &op), 0, "no seal means no index entry");
		assert_eq!(anchor_of(&mut txn, &op, group), None, "and no anchor");
	}

	#[test]
	fn append_reports_no_operator_sample() {
		// Append's mappings live in the shared row-number registry, so a mapping leak here is
		// attributed through the registry's per-operator metrics, not a per-operator sample.
		assert!(HostOperator::sample(&op(11)).is_none(), "append has no owned operator state to sample");
	}
}
