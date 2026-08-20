// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::{decode_u64, encode_u64, encoded::EncodedKey, serializer::KeySerializer},
	row::operator::state::OperatorState,
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
		flow::OperatorCapability,
	},
	key::operator_state::{GroupId, GroupStateKey},
	metrics::{heap::OperatorSample, instruments::counter::Counter},
	state::timer::TimerKind,
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::instrument;

use crate::{
	error::{FlowGraphError, FlowStateError},
	operator::{
		HostOperator,
		drops::SealedDrops,
		host::HostContext,
		state::{
			reaper::{StoreReaper, drain, drain_group, enqueue, queue_key, queued},
			seal::{ledger::FiredAt, policy::SealPolicy},
		},
	},
	timer::Timer,
	transaction::anchor::{SealAnchor, UNGROUPED_SIDE, anchor_key as seal_anchor_key},
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

const REMOVE_RECLAIM_LIMIT: usize = 8;

const SEAL_BATCH: usize = 256;

const QUEUE_SWEEP_EVERY: u64 = 1024;

const DROP_REASON: &str = "mutations whose source row mapping was reclaimed";

pub struct AppendOperator {
	operator: OperatorId,

	parent_schema: Option<Columns>,

	input_nodes: Vec<OperatorId>,

	dropped: SealedDrops,

	lateness: Option<Duration>,

	seal_fires: Counter,
}

impl AppendOperator {
	pub fn new(
		operator: OperatorId,
		parent_schema: Option<Columns>,
		input_nodes: Vec<OperatorId>,
		lateness: Option<Duration>,
	) -> Self {
		reifydb_assertions! {
			assert!(input_nodes.len() >= 2, "Append requires at least 2 inputs");
		}

		Self {
			operator,
			parent_schema,
			input_nodes,
			dropped: SealedDrops::new(operator, DROP_REASON),
			lateness: lateness.filter(|span| !span.is_zero()),
			seal_fires: Counter::new("flow.operator.append.seal_fires_total", "Append seal timer fires"),
		}
	}

	#[cfg(test)]
	pub(crate) fn new_for_state_tests(operator: OperatorId) -> Self {
		Self {
			operator,
			parent_schema: None,
			input_nodes: Vec::new(),
			dropped: SealedDrops::new(operator, DROP_REASON),
			lateness: None,
			seal_fires: Counter::new("flow.operator.append.seal_fires_total", "Append seal timer fires"),
		}
	}

	#[cfg(test)]
	pub(crate) fn sealing_for_state_tests(operator: OperatorId, lateness: Duration) -> Self {
		Self {
			lateness: Some(lateness),
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
		seal_anchor_key(group, UNGROUPED_SIDE, RowNumber(0))
	}

	fn timer_key(group: GroupId) -> EncodedKey {
		EncodedKey::new(encode_u64(group.0))
	}

	fn timer_group(key: &EncodedKey) -> Result<GroupId> {
		let bytes = <[u8; 8]>::try_from(key.as_slice()).map_err(|_| {
			Error::from(FlowStateError::Decode {
				state: "append seal timer key",
				cause: format!("expected eight group bytes, found {}", key.as_slice().len()),
			})
		})?;
		Ok(GroupId(decode_u64(bytes)))
	}

	fn read_anchor(host: &mut dyn HostContext, group: GroupId) -> Result<Option<DateTime>> {
		host.anchor_at(group, UNGROUPED_SIDE, RowNumber(0))
	}

	fn arm_seal(&mut self, host: &mut dyn HostContext, groups: &[EncodedKey], columns: &Columns) -> Result<()> {
		let Some(lateness) = self.lateness else {
			return Ok(());
		};
		let policy = SealPolicy::of(lateness);
		let times = columns.time().to_vec();
		for (index, resolved) in host.lookup_groups(groups)?.into_iter().enumerate() {
			let (Some(group), Some(at)) = (resolved, times.get(index)) else {
				continue;
			};
			self.move_anchor(host, group, policy.seal_instant(*at).at())?;
		}
		Ok(())
	}

	fn move_anchor(&mut self, host: &mut dyn HostContext, group: GroupId, expiry: DateTime) -> Result<()> {
		let prior = Self::read_anchor(host, group)?;
		if prior == Some(expiry) {
			return Ok(());
		}
		if prior.is_some() {
			host.state_remove(&queue_key(group))?;
		}
		host.state_set(
			&Self::anchor_key(group),
			SealAnchor {
				expiry,
			}
			.encode_state()?,
		)?;
		host.arm_timer(expiry, TimerKind::Maintenance, &Self::timer_key(group))
	}

	fn clear_seal(&mut self, host: &mut dyn HostContext, groups: &[GroupId]) -> Result<()> {
		if self.lateness.is_none() {
			return Ok(());
		}
		for group in groups {
			let Some(expiry) = Self::read_anchor(host, *group)? else {
				continue;
			};
			host.state_remove(&Self::anchor_key(*group))?;
			host.disarm_timer(expiry, TimerKind::Maintenance, &Self::timer_key(*group))?;
		}
		Ok(())
	}

	fn seal_group(&mut self, host: &mut dyn HostContext, fired: FiredAt, group: GroupId) -> Result<()> {
		let Some(lateness) = self.lateness else {
			return Ok(());
		};
		let retry = fired.at().saturating_add(lateness);

		self.seal_fires.inc();
		if (self.seal_fires.get() as u64).is_multiple_of(QUEUE_SWEEP_EVERY) {
			let drained = drain(host, &mut StoreReaper, SEAL_BATCH)?;
			let pending = if drained.more {
				queued(host, SEAL_BATCH)?.groups
			} else {
				drained.still_queued
			};
			for stalled in pending {
				host.arm_timer(retry, TimerKind::Maintenance, &Self::timer_key(stalled))?;
			}
		}

		let Some(expiry) = Self::read_anchor(host, group)? else {
			return Ok(());
		};
		if expiry > fired.at() {
			host.arm_timer(expiry, TimerKind::Maintenance, &Self::timer_key(group))?;
			return Ok(());
		}
		host.state_remove(&Self::anchor_key(group))?;
		enqueue(host, group)?;

		let drained = drain_group(host, group, &mut StoreReaper, SEAL_BATCH)?;
		if drained.still_queued {
			host.arm_timer(retry, TimerKind::Maintenance, &Self::timer_key(group))?;
		}
		Ok(())
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
			self.seal_group(host, FiredAt::of(&timer), Self::timer_group(&timer.key)?)?;
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
		let mapping = Self::mapping_key();
		let fresh: Vec<GroupId> =
			interned.iter().filter(|(_, is_new)| *is_new).map(|(group, _)| *group).collect();
		let mut minted = host.create_row_numbers(&fresh, &mapping)?.into_iter();

		let known: Vec<GroupId> =
			interned.iter().filter(|(_, is_new)| !*is_new).map(|(group, _)| *group).collect();
		let mut looked_up = host.get_or_create_row_numbers_for_groups(&known, &mapping)?.into_iter();

		let mut output_row_numbers = Vec::with_capacity(interned.len());
		for (_, is_new) in interned {
			let output_row_number = match is_new {
				true => minted.next().expect("one row number is minted per freshly interned group"),
				false => {
					looked_up
						.next()
						.expect("one row number is resolved per already-interned group")
						.0
				}
			};
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
		let mut ids = Vec::with_capacity(groups.len());
		for resolved in host.lookup_groups(groups)? {
			let Some(group) = resolved else {
				return Ok(None);
			};
			ids.push(group);
		}

		let mut output_row_numbers = Vec::with_capacity(ids.len());
		for row_number in host.get_row_numbers_for_groups(&ids, &Self::mapping_key())? {
			let Some(row_number) = row_number else {
				return Ok(None);
			};
			output_row_numbers.push(row_number);
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
		let output = pre.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::remove(output)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		key::operator_state::{Keyspace, group_inner_range, keyspace_inner_range},
		value::column::columns::Columns,
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{factory::time::at_millis, value::datetime::DateTime};

	use super::*;
	use crate::{
		operator::host::TxnHostContext,
		timer::extension::TimerExtension,
		transaction::{
			ChangeCoordinate, FlowTransaction,
			deferred::DeferredTransaction,
			group::GroupExtension,
			mock::FlowTxn,
			row_number::RowNumberExtension,
			state::{StateExtension, StateRange},
			substrate::apply_operator_state,
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

	fn fire(op: &mut AppendOperator, txn: &mut DeferredTransaction, due: DateTime, group: GroupId) {
		// The engine lifts a due timer off the wheel before dispatch, so skipping the disarm reads as a leak.
		let operator = op.operator;
		let timer = Timer {
			due,
			kind: TimerKind::Maintenance,
			key: AppendOperator::timer_key(group),
		};
		txn.disarm_timer(operator, &timer).unwrap();
		op.on_timer(&mut TxnHostContext::new(txn, operator), timer).unwrap();
	}

	fn armed_timers(txn: &mut DeferredTransaction, op: &AppendOperator) -> usize {
		// The wheel is the only due-ordered schedule, so a stale or duplicated arming shows up exactly here.
		txn.state_range(
			op.operator,
			StateRange::forward(keyspace_inner_range(GroupId::ROOT, Keyspace::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.len()
	}

	fn anchor_of(txn: &mut DeferredTransaction, op: &AppendOperator, group: GroupId) -> Option<DateTime> {
		AppendOperator::read_anchor(&mut host(txn, op), group).unwrap()
	}

	fn commit(engine: &TestEngine, txn: &mut DeferredTransaction) {
		// Anchors only reach the typed table through the batch, and the seal path reads them differently there.
		apply_operator_state(&engine.inner().operator_state(), &txn.take_pending());
	}

	fn group_of(
		txn: &mut DeferredTransaction,
		op: &AppendOperator,
		parent: u8,
		source_row: u64,
	) -> Option<GroupId> {
		txn.lookup_groups(op.operator, &[AppendOperator::group_bytes(parent, RowNumber(source_row))])
			.unwrap()
			.remove(0)
	}

	fn group_rows(txn: &mut DeferredTransaction, op: &AppendOperator, group: GroupId) -> usize {
		txn.state_range(op.operator, StateRange::forward(group_inner_range(group), "test")).unwrap().items.len()
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
			txn.get_row_numbers(op.operator, group, &[AppendOperator::mapping_key()]).unwrap().remove(0),
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
	fn a_source_row_repeated_inside_one_batch_lands_on_one_output_row() {
		// Only the first occurrence interns a group, so the repeat must resolve to the number just minted.
		let engine = TestEngine::new();
		let op = op(4);
		let mut txn = txn_at(&engine, op.operator, 100);

		let assigned = op
			.translate_create_row_numbers(
				&mut host(&mut txn, &op),
				&AppendOperator::group_keys(0, &rows(&[7, 9, 7])),
			)
			.unwrap();

		assert_eq!(assigned[0], assigned[2], "both slots of source row 7 must carry one output row number");
		assert_ne!(assigned[0], assigned[1], "a distinct source row keeps a distinct output row number");
	}

	#[test]
	fn a_batch_mixing_a_known_source_row_with_fresh_ones_keeps_every_slot_aligned() {
		// Minted and looked-up numbers must re-interleave in input order, or a row takes its neighbour's.
		let engine = TestEngine::new();
		let op = op(5);
		let mut txn = txn_at(&engine, op.operator, 100);

		let known = op
			.translate_create_row_numbers(
				&mut host(&mut txn, &op),
				&AppendOperator::group_keys(0, &rows(&[7])),
			)
			.unwrap();
		let assigned = op
			.translate_create_row_numbers(
				&mut host(&mut txn, &op),
				&AppendOperator::group_keys(0, &rows(&[5, 7, 9])),
			)
			.unwrap();

		assert_eq!(assigned[1], known[0], "the known source row keeps its number in the slot it arrived in");
		assert_ne!(assigned[0], assigned[2], "the two fresh rows take numbers of their own");
		for (slot, source) in [(0usize, 5u64), (2, 9)] {
			let group = group_of(&mut txn, &op, 0, source).expect("a fresh source row interns a group");
			assert_eq!(
				txn.get_row_numbers(op.operator, group, &[AppendOperator::mapping_key()])
					.unwrap()
					.remove(0),
				Some(assigned[slot]),
				"each fresh row's number must be stored under its own group"
			);
		}
	}

	#[test]
	fn a_known_source_row_keeps_its_number_when_it_returns_beside_fresh_rows() {
		// The mapping is read back from the store here, so re-minting would duplicate the sink row.
		let engine = TestEngine::new();
		let op = op(6);
		let mut txn = txn_at(&engine, op.operator, 100);
		let known = op
			.translate_create_row_numbers(
				&mut host(&mut txn, &op),
				&AppendOperator::group_keys(0, &rows(&[7])),
			)
			.unwrap();
		commit(&engine, &mut txn);

		let mut later = txn_at(&engine, op.operator, 200);
		let assigned = op
			.translate_create_row_numbers(
				&mut host(&mut later, &op),
				&AppendOperator::group_keys(0, &rows(&[3, 7])),
			)
			.unwrap();

		assert_eq!(assigned[1], known[0], "a persisted source row must not be re-numbered");
		assert_ne!(assigned[0], known[0], "the fresh row beside it takes a number of its own");
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
	fn an_inserted_row_is_armed_one_lateness_past_its_own_event_time() {
		// The anchor must be the row's own event time; a wall-clock lateness evicts a backfilled row on
		// arrival.
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
			"the due time is event time + lateness + the strict gate step"
		);
		assert_eq!(armed_timers(&mut txn, &op), 1, "and exactly one timer addresses that row");
	}

	#[test]
	fn an_update_moves_the_rows_timer_rather_than_adding_a_second() {
		// Without cancelling the old arming the row is addressed twice and the stale one fires while it lives.
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
		assert_eq!(armed_timers(&mut txn, &op), 1, "an update re-arms one timer, it does not add one");
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

		fire(&mut op, &mut txn, at_millis(15_001), group);

		assert_eq!(group_of(&mut txn, &op, 0, 42), None, "the dictionary entry must go");
		assert_eq!(group_rows(&mut txn, &op, group), 0, "the group's range must be left empty");
		assert_eq!(armed_timers(&mut txn, &op), 0, "and the timer that drove the seal must not re-arm");
	}

	#[test]
	fn a_committed_anchor_leaves_no_row_behind_once_its_group_seals() {
		// The reaper sweeps the key-value rows only, so an anchor in the typed table outlives every group it
		// seals.
		let engine = TestEngine::new();
		let store = engine.inner().operator_state();
		let mut op = sealing(28);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		let group = group_of(&mut txn, &op, 0, 42).expect("precondition: the row is interned");
		commit(&engine, &mut txn);
		assert_eq!(
			store.anchors_by_expiry(op.operator, group, 16).len(),
			1,
			"precondition: the anchor is in the table, not merely in the batch"
		);

		fire(&mut op, &mut txn, at_millis(15_001), group);
		commit(&engine, &mut txn);

		assert_eq!(
			store.anchors_by_expiry(op.operator, group, 16),
			Vec::new(),
			"a group driven through seal, enqueue and drain must leave no anchor row"
		);
	}

	#[test]
	fn a_row_still_inside_its_lateness_survives_a_maintenance_tick() {
		// The gate is strict: a row whose due time lands exactly on the tick must not seal yet.
		let engine = TestEngine::new();
		let mut op = sealing(23);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		let group = group_of(&mut txn, &op, 0, 42).expect("precondition: the row is interned");

		fire(&mut op, &mut txn, at_millis(15_000), group);

		let group =
			group_of(&mut txn, &op, 0, 42).expect("a row one millisecond short of its lateness must live");
		assert!(group_rows(&mut txn, &op, group) > 0, "and must keep the state that resolves it");
		assert_eq!(armed_timers(&mut txn, &op), 1, "its timer is not due and must stay armed");
	}

	#[test]
	fn a_mutation_arriving_after_the_lateness_is_counted_rather_than_translated() {
		// A sealed row's published row is frozen, so the discarded mutation must be counted or it vanishes
		// silently.
		let engine = TestEngine::new();
		let mut op = sealing(24);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		let group = group_of(&mut txn, &op, 0, 42).expect("precondition: the row is interned");
		fire(&mut op, &mut txn, at_millis(15_001), group);

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
	fn removing_a_row_takes_its_timer_and_its_anchor_with_it() {
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

		assert_eq!(armed_timers(&mut txn, &op), 0, "the timer must go with the row");
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
			queued(&mut host(&mut txn, &op), 16).unwrap().groups.is_empty(),
			"a re-armed row must not be left waiting in the reap queue"
		);
	}

	#[test]
	fn an_operator_without_a_lateness_arms_nothing_at_all() {
		// Arming without a lateness leaves one timer and one anchor per row that nothing ever collects.
		let engine = TestEngine::new();
		let mut op = op(27);
		let mut txn = txn_at(&engine, op.operator, 100);

		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		let group = group_of(&mut txn, &op, 0, 42).expect("the row must still intern a group");
		assert_eq!(armed_timers(&mut txn, &op), 0, "no lateness means no timer");
		assert_eq!(anchor_of(&mut txn, &op, group), None, "and no anchor");
	}

	#[test]
	fn append_reports_no_operator_sample() {
		// Append's mappings live in the shared row-number registry, so a mapping leak here is
		// attributed through the registry's per-operator metrics, not a per-operator sample.
		assert!(HostOperator::sample(&op(11)).is_none(), "append has no owned operator state to sample");
	}
}
