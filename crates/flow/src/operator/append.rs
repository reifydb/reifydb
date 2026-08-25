// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::{deserializer::KeyDeserializer, encoded::EncodedKey, serializer::KeySerializer},
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
		state::seal::{ledger::FiredAt, policy::SealPolicy},
	},
	timer::Timer,
	transaction::anchor::{SealAnchor, anchor_key as seal_anchor_key},
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

const DROP_REASON: &str = "mutations whose source row mapping was reclaimed";

pub struct AppendOperator {
	operator: OperatorId,

	parent_schema: Option<Columns>,

	input_nodes: Vec<OperatorId>,

	dropped: SealedDrops,

	retention: Option<Duration>,

	seal_fires: Counter,
}

impl AppendOperator {
	pub fn new(
		operator: OperatorId,
		parent_schema: Option<Columns>,
		input_nodes: Vec<OperatorId>,
		retention: Option<Duration>,
	) -> Self {
		reifydb_assertions! {
			assert!(input_nodes.len() >= 2, "Append requires at least 2 inputs");
			assert!(
				input_nodes.len() <= u8::MAX as usize + 1,
				"the input index is one key byte in both the row-number mapping key and the seal \
				 anchor side, so a 257th input would alias input 0 and take over its rows"
			);
		}

		Self {
			operator,
			parent_schema,
			input_nodes,
			dropped: SealedDrops::new(operator, DROP_REASON),
			retention: retention.filter(|span| !span.is_zero()),
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
			retention: None,
			seal_fires: Counter::new("flow.operator.append.seal_fires_total", "Append seal timer fires"),
		}
	}

	#[cfg(test)]
	pub(crate) fn sealing_for_state_tests(operator: OperatorId, retention: Duration) -> Self {
		Self {
			retention: Some(retention),
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

	fn append_key(parent_index: u8, source_row: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(parent_index);
		serializer.extend_u64(source_row.0);
		serializer.finish()
	}

	fn append_keys(parent_index: u8, source: &Columns) -> Vec<EncodedKey> {
		(0..source.row_count())
			.map(|row_idx| Self::append_key(parent_index, source.row_numbers()[row_idx]))
			.collect()
	}

	fn append_parts(key: &EncodedKey) -> Result<(u8, RowNumber)> {
		let mut reader = KeyDeserializer::from_bytes(key.as_slice());
		let parent_index = reader.read_u8().map_err(|_| Self::undecodable(key))?;
		let source_row = reader.read_u64().map_err(|_| Self::undecodable(key))?;
		Ok((parent_index, RowNumber(source_row)))
	}

	fn undecodable(key: &EncodedKey) -> Error {
		Error::from(FlowStateError::Decode {
			state: "append seal timer key",
			cause: format!(
				"expected an input byte and eight source row bytes, found {}",
				key.as_slice().len()
			),
		})
	}

	fn anchor_key(parent_index: u8, source_row: RowNumber) -> GroupStateKey {
		seal_anchor_key(GroupId::ROOT, parent_index, source_row)
	}

	fn read_anchor(
		host: &mut dyn HostContext,
		parent_index: u8,
		source_row: RowNumber,
	) -> Result<Option<DateTime>> {
		host.anchor_at(GroupId::ROOT, parent_index, source_row)
	}

	fn arm_seal(
		&mut self,
		host: &mut dyn HostContext,
		parent_index: u8,
		source_rows: &[RowNumber],
		columns: &Columns,
	) -> Result<()> {
		let Some(retention) = self.retention else {
			return Ok(());
		};
		let policy = SealPolicy::of(retention);
		let times = columns.time().to_vec();
		for (index, source_row) in source_rows.iter().enumerate() {
			let Some(at) = times.get(index) else {
				continue;
			};
			self.move_anchor(host, parent_index, *source_row, policy.seal_instant(*at).at())?;
		}
		Ok(())
	}

	fn move_anchor(
		&mut self,
		host: &mut dyn HostContext,
		parent_index: u8,
		source_row: RowNumber,
		expiry: DateTime,
	) -> Result<()> {
		if Self::read_anchor(host, parent_index, source_row)? == Some(expiry) {
			return Ok(());
		}
		host.state_set(
			&Self::anchor_key(parent_index, source_row),
			SealAnchor {
				expiry,
			}
			.encode_state()?,
		)?;
		host.arm_timer(expiry, TimerKind::Maintenance, &Self::append_key(parent_index, source_row))
	}

	fn clear_seal(
		&mut self,
		host: &mut dyn HostContext,
		parent_index: u8,
		source_rows: &[RowNumber],
	) -> Result<()> {
		if self.retention.is_none() {
			return Ok(());
		}
		for source_row in source_rows {
			let Some(expiry) = Self::read_anchor(host, parent_index, *source_row)? else {
				continue;
			};
			host.state_remove(&Self::anchor_key(parent_index, *source_row))?;
			host.disarm_timer(
				expiry,
				TimerKind::Maintenance,
				&Self::append_key(parent_index, *source_row),
			)?;
		}
		Ok(())
	}

	fn seal_row(&mut self, host: &mut dyn HostContext, fired: FiredAt, key: &EncodedKey) -> Result<()> {
		if self.retention.is_none() {
			return Ok(());
		}
		let (parent_index, source_row) = Self::append_parts(key)?;

		self.seal_fires.inc();

		let Some(expiry) = Self::read_anchor(host, parent_index, source_row)? else {
			return Ok(());
		};
		if expiry > fired.at() {
			host.arm_timer(expiry, TimerKind::Maintenance, key)?;
			return Ok(());
		}
		host.state_remove(&Self::anchor_key(parent_index, source_row))?;
		host.remove_row_number(GroupId::ROOT, key)
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
			self.seal_row(host, FiredAt::of(&timer), &timer.key)?;
		}
		Ok(None)
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

impl AppendOperator {
	#[inline]
	#[instrument(name = "flow::operator::append::create_row_numbers", level = "trace", skip_all, fields(rows = keys.len()))]
	fn translate_create_row_numbers(
		&self,
		host: &mut dyn HostContext,
		keys: &[EncodedKey],
	) -> Result<Vec<RowNumber>> {
		Ok(host.get_or_create_row_numbers(GroupId::ROOT, keys)?
			.into_iter()
			.map(|(row_number, _)| row_number)
			.collect())
	}

	#[inline]
	#[instrument(name = "flow::operator::append::lookup_row_numbers", level = "trace", skip_all, fields(rows = keys.len()))]
	fn lookup_row_numbers(
		&self,
		host: &mut dyn HostContext,
		keys: &[EncodedKey],
	) -> Result<Option<Vec<RowNumber>>> {
		let mut output_row_numbers = Vec::with_capacity(keys.len());
		for row_number in host.get_row_numbers(GroupId::ROOT, keys)? {
			let Some(row_number) = row_number else {
				return Ok(None);
			};
			output_row_numbers.push(row_number);
		}
		Ok(Some(output_row_numbers))
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
		let parent_index = parent_index as u8;
		let keys = Self::append_keys(parent_index, &post);
		let output_row_numbers = self.translate_create_row_numbers(host, &keys)?;
		let source_rows = post.row_numbers().to_vec();
		self.arm_seal(host, parent_index, &source_rows, &post)?;
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
		let parent_index = parent_index as u8;
		let keys = Self::append_keys(parent_index, &pre);
		let Some(output_row_numbers) = self.lookup_row_numbers(host, &keys)? else {
			self.dropped.note(post.row_count() as u64);
			return Ok(None);
		};
		let source_rows = pre.row_numbers().to_vec();
		self.arm_seal(host, parent_index, &source_rows, &post)?;
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
		let parent_index = parent_index as u8;
		let keys = Self::append_keys(parent_index, &pre);
		let Some(output_row_numbers) = self.lookup_row_numbers(host, &keys)? else {
			self.dropped.note(pre.row_count() as u64);
			return Ok(None);
		};
		let source_rows = pre.row_numbers().to_vec();
		self.clear_seal(host, parent_index, &source_rows)?;
		for key in &keys {
			host.remove_row_number(GroupId::ROOT, key)?;
		}
		let output = pre.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::remove(output)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		key::operator_state::{Keyspace, keyspace_inner_range},
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

	fn keys(parent: u8, source_rows: &[u64]) -> Vec<EncodedKey> {
		AppendOperator::append_keys(parent, &rows(source_rows))
	}

	fn fire(op: &mut AppendOperator, txn: &mut DeferredTransaction, due: DateTime, parent: u8, source_row: u64) {
		// The engine lifts a due timer off the wheel before dispatch, so skipping the disarm reads as a leak.
		let operator = op.operator;
		let timer = Timer {
			due,
			kind: TimerKind::Maintenance,
			key: AppendOperator::append_key(parent, RowNumber(source_row)),
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

	fn anchor_of(
		txn: &mut DeferredTransaction,
		op: &AppendOperator,
		parent: u8,
		source_row: u64,
	) -> Option<DateTime> {
		AppendOperator::read_anchor(&mut host(txn, op), parent, RowNumber(source_row)).unwrap()
	}

	fn mapped_row(
		txn: &mut DeferredTransaction,
		op: &AppendOperator,
		parent: u8,
		source_row: u64,
	) -> Option<RowNumber> {
		// A source row owns exactly one mapping row, addressed by its own key; reading it back is the
		// only way to tell a live row from one whose identity was reclaimed.
		txn.get_row_numbers(
			op.operator,
			GroupId::ROOT,
			&[AppendOperator::append_key(parent, RowNumber(source_row))],
		)
		.unwrap()
		.remove(0)
	}

	fn commit(engine: &TestEngine, txn: &mut DeferredTransaction) {
		// Anchors only reach the typed table through the batch, and the seal path reads them differently there.
		apply_operator_state(&engine.inner().operator_state(), &txn.take_pending());
	}

	#[test]
	fn a_source_row_maps_its_key_to_its_output_row_number() {
		// The mapping is addressed by the source row's own key, which is what lets the seal and remove
		// paths find and delete it; written anywhere else it would be unreachable and leak one row per
		// source row for the life of the operator.
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

		assert_eq!(
			mapped_row(&mut txn, &op, 0, 42),
			Some(post.row_numbers()[0]),
			"the output row number must be readable back from the source row's own key"
		);
	}

	#[test]
	fn the_same_source_row_always_translates_to_the_same_output_row() {
		// A source row keeps one identity downstream for as long as the mapping lives; a second
		// insert that minted a fresh number would duplicate the sink row rather than replace it.
		let engine = TestEngine::new();
		let op = op(2);
		let mut txn = txn_at(&engine, op.operator, 100);

		let first = op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[7])).unwrap();
		let second = op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[7])).unwrap();

		assert_eq!(first, second, "an already-mapped source row must resolve to its existing output row");
	}

	#[test]
	fn each_input_numbers_its_own_source_rows_independently() {
		// A union's inputs number their rows independently, so the parent index has to be in the
		// mapping key: sharing one key would collapse two unrelated source rows onto a single output
		// row and let either input's reclamation erase the other's mapping.
		let engine = TestEngine::new();
		let op = op(3);
		let mut txn = txn_at(&engine, op.operator, 100);

		let left = op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[7])).unwrap();
		let right = op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(1, &[7])).unwrap();

		assert_ne!(
			AppendOperator::append_key(0, RowNumber(7)),
			AppendOperator::append_key(1, RowNumber(7)),
			"the same source row number on two inputs must not share a mapping key"
		);
		assert_ne!(left, right, "and must not share an output row number");
	}

	#[test]
	fn a_source_row_repeated_inside_one_batch_lands_on_one_output_row() {
		// Only the first occurrence creates the mapping, so the repeat must resolve to the number just minted.
		let engine = TestEngine::new();
		let op = op(4);
		let mut txn = txn_at(&engine, op.operator, 100);

		let assigned = op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[7, 9, 7])).unwrap();

		assert_eq!(assigned[0], assigned[2], "both slots of source row 7 must carry one output row number");
		assert_ne!(assigned[0], assigned[1], "a distinct source row keeps a distinct output row number");
	}

	#[test]
	fn a_batch_mixing_a_known_source_row_with_fresh_ones_keeps_every_slot_aligned() {
		// Minted and looked-up numbers must re-interleave in input order, or a row takes its neighbour's.
		let engine = TestEngine::new();
		let op = op(5);
		let mut txn = txn_at(&engine, op.operator, 100);

		let known = op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[7])).unwrap();
		let assigned = op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[5, 7, 9])).unwrap();

		assert_eq!(assigned[1], known[0], "the known source row keeps its number in the slot it arrived in");
		assert_ne!(assigned[0], assigned[2], "the two fresh rows take numbers of their own");
		for (slot, source) in [(0usize, 5u64), (2, 9)] {
			assert_eq!(
				mapped_row(&mut txn, &op, 0, source),
				Some(assigned[slot]),
				"each fresh row's number must be stored under its own key"
			);
		}
	}

	#[test]
	fn a_known_source_row_keeps_its_number_when_it_returns_beside_fresh_rows() {
		// The mapping is read back from the store here, so re-minting would duplicate the sink row.
		let engine = TestEngine::new();
		let op = op(6);
		let mut txn = txn_at(&engine, op.operator, 100);
		let known = op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[7])).unwrap();
		commit(&engine, &mut txn);

		let mut later = txn_at(&engine, op.operator, 200);
		let assigned = op.translate_create_row_numbers(&mut host(&mut later, &op), &keys(0, &[3, 7])).unwrap();

		assert_eq!(assigned[1], known[0], "a persisted source row must not be re-numbered");
		assert_ne!(assigned[0], known[0], "the fresh row beside it takes a number of its own");
	}

	#[test]
	fn a_source_row_that_was_never_seen_is_looked_up_never_created() {
		// If lookup created the mapping, every unmatched diff would leave a row-number entry behind
		// addressing a source row the operator never accepted - unbounded growth driven entirely by
		// traffic it drops on the floor.
		let engine = TestEngine::new();
		let mut op = op(4);
		let mut txn = txn_at(&engine, op.operator, 100);

		assert!(op
			.translate_append_update(&mut host(&mut txn, &op), 0, rows(&[99]), rows(&[99]))
			.unwrap()
			.is_none());
		assert!(op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[99])).unwrap().is_none());

		assert_eq!(mapped_row(&mut txn, &op, 0, 99), None, "a lookup must not have mapped the missing row");
	}

	#[test]
	fn a_partly_known_batch_translates_to_nothing_at_all() {
		// The diff carries one Columns for the whole batch, so it is all-or-nothing: emitting only
		// the rows that resolved hands the sink row numbers that no longer line up with the values.
		let engine = TestEngine::new();
		let mut op = op(5);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[1])).unwrap();

		assert!(op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[1, 2])).unwrap().is_none());
		assert!(
			mapped_row(&mut txn, &op, 0, 1).is_some(),
			"the row that did resolve must not have been reclaimed by a batch that failed"
		);
	}

	#[test]
	fn a_row_whose_mapping_was_reclaimed_translates_to_nothing() {
		// Retention frees rows one at a time, so a batch can straddle the moment one of its rows was
		// sealed. The sealed row resolves to no number, which is the other half of the all-or-nothing
		// rule, and the live row beside it must not be touched.
		let engine = TestEngine::new();
		let mut op = op(12);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[1, 2])).unwrap();
		txn.remove_row_number(op.operator, GroupId::ROOT, &AppendOperator::append_key(0, RowNumber(2)))
			.unwrap();
		assert!(mapped_row(&mut txn, &op, 0, 2).is_none(), "the sealed row must have no mapping left");

		assert!(op
			.translate_append_update(&mut host(&mut txn, &op), 0, rows(&[1, 2]), rows(&[1, 2]))
			.unwrap()
			.is_none());
		assert!(op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[1, 2])).unwrap().is_none());
		assert!(
			mapped_row(&mut txn, &op, 0, 1).is_some(),
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

		op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[7])).unwrap();
		op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[7]))
			.unwrap()
			.expect("a known row must translate");
		assert_eq!(op.dropped.total(), 5, "a mutation that did translate must not be counted as a drop");
	}

	#[test]
	fn removing_a_source_row_takes_its_mapping_with_it() {
		// The mapping is the only state a retention-free append owns, so leaving it behind is one
		// permanently orphaned row per removed source row, addressed by a source row that is gone.
		let engine = TestEngine::new();
		let mut op = op(6);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_create_row_numbers(&mut host(&mut txn, &op), &keys(0, &[5])).unwrap();
		assert!(mapped_row(&mut txn, &op, 0, 5).is_some(), "precondition: the row is mapped");

		op.translate_append_remove(&mut host(&mut txn, &op), 0, rows(&[5]))
			.unwrap()
			.expect("a known row must translate");

		assert_eq!(mapped_row(&mut txn, &op, 0, 5), None, "the mapping must go with the row");
	}

	#[test]
	fn an_inserted_row_is_armed_one_retention_past_its_own_event_time() {
		// anchor must be the row's own event time, not wall-clock, or a backfilled row evicts on arrival
		let engine = TestEngine::new();
		let mut op = sealing(20);
		let mut txn = txn_at(&engine, op.operator, 100);

		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		assert_eq!(
			anchor_of(&mut txn, &op, 0, 42),
			Some(at_millis(15_001)),
			"the due time is event time + retention + the strict gate step"
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

		assert_eq!(armed_timers(&mut txn, &op), 1, "an update re-arms one timer, it does not add one");
		assert_eq!(anchor_of(&mut txn, &op, 0, 7), Some(at_millis(30_001)), "and the due time follows the row");
	}

	#[test]
	fn a_sealed_row_loses_its_mapping_and_its_anchor() {
		// The mapping and the anchor are the whole of what append owns per row, so a seal that frees
		// neither leaks both for every source row that ever passed through.
		let engine = TestEngine::new();
		let mut op = sealing(22);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		assert!(mapped_row(&mut txn, &op, 0, 42).is_some(), "precondition: the row is mapped");

		fire(&mut op, &mut txn, at_millis(15_001), 0, 42);

		assert_eq!(mapped_row(&mut txn, &op, 0, 42), None, "the mapping must go");
		assert_eq!(anchor_of(&mut txn, &op, 0, 42), None, "and the anchor behind it");
		assert_eq!(armed_timers(&mut txn, &op), 0, "and the timer that drove the seal must not re-arm");
	}

	#[test]
	fn a_committed_anchor_leaves_no_row_behind_once_its_row_seals() {
		// An anchor that reached the typed table is invisible to the key-value paths, so a seal that
		// only clears the batch copy outlives every row it sealed.
		let engine = TestEngine::new();
		let store = engine.inner().operator_state();
		let mut op = sealing(28);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		commit(&engine, &mut txn);
		assert_eq!(
			store.anchors_by_expiry(op.operator, GroupId::ROOT, 16).len(),
			1,
			"precondition: the anchor is in the table, not merely in the batch"
		);

		fire(&mut op, &mut txn, at_millis(15_001), 0, 42);
		commit(&engine, &mut txn);

		assert_eq!(
			store.anchors_by_expiry(op.operator, GroupId::ROOT, 16),
			Vec::new(),
			"a row driven through its seal must leave no anchor row"
		);
	}

	#[test]
	fn a_row_still_inside_its_retention_survives_a_maintenance_tick() {
		// The gate is strict: a row whose due time lands exactly on the tick must not seal yet.
		let engine = TestEngine::new();
		let mut op = sealing(23);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		fire(&mut op, &mut txn, at_millis(15_000), 0, 42);

		assert!(
			mapped_row(&mut txn, &op, 0, 42).is_some(),
			"a row one millisecond short of its retention must live"
		);
		assert_eq!(
			anchor_of(&mut txn, &op, 0, 42),
			Some(at_millis(15_001)),
			"and must keep the anchor that seals it"
		);
		assert_eq!(armed_timers(&mut txn, &op), 1, "its timer is not due and must stay armed");
	}

	#[test]
	fn a_mutation_arriving_after_the_retention_is_counted_rather_than_translated() {
		// A sealed row's published row is frozen, so the discarded mutation must be counted or it vanishes
		// silently.
		let engine = TestEngine::new();
		let mut op = sealing(24);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");
		fire(&mut op, &mut txn, at_millis(15_001), 0, 42);

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
		// The anchor and the timer outlive the mapping unless the remove path clears them explicitly,
		// and the timer would then fire on a row that no longer exists.
		let engine = TestEngine::new();
		let mut op = sealing(25);
		let mut txn = txn_at(&engine, op.operator, 100);
		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[5], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		op.translate_append_remove(&mut host(&mut txn, &op), 0, timed(&[5], at_millis(5_000)))
			.unwrap()
			.expect("a known row must translate");

		assert_eq!(armed_timers(&mut txn, &op), 0, "the timer must go with the row");
		assert_eq!(anchor_of(&mut txn, &op, 0, 5), None, "and so must the anchor behind it");
	}

	#[test]
	fn a_timer_that_fires_after_its_row_was_extended_re_arms_rather_than_seals() {
		// An update moves the anchor forward but the old arming is still on the wheel, so the stale
		// firing must re-arm on the new due time. Sealing on it would reclaim a row that is still
		// well inside its retention, and the sink would lose a live row with no drop counted.
		let engine = TestEngine::new();
		let mut op = sealing(26);
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

		fire(&mut op, &mut txn, at_millis(15_001), 0, 7);

		assert!(mapped_row(&mut txn, &op, 0, 7).is_some(), "the extended row must survive its stale timer");
		assert_eq!(
			anchor_of(&mut txn, &op, 0, 7),
			Some(at_millis(30_001)),
			"and keep the due time the update gave it"
		);
		assert_eq!(armed_timers(&mut txn, &op), 1, "which must be re-armed rather than dropped");
	}

	#[test]
	fn an_operator_without_a_retention_arms_nothing_at_all() {
		// Arming without a retention leaves one timer and one anchor per row that nothing ever collects.
		let engine = TestEngine::new();
		let mut op = op(27);
		let mut txn = txn_at(&engine, op.operator, 100);

		op.translate_append_insert(&mut host(&mut txn, &op), 0, timed(&[42], at_millis(5_000)))
			.unwrap()
			.expect("an insert must translate");

		assert!(mapped_row(&mut txn, &op, 0, 42).is_some(), "the row must still be mapped");
		assert_eq!(armed_timers(&mut txn, &op), 0, "no retention means no timer");
		assert_eq!(anchor_of(&mut txn, &op, 0, 42), None, "and no anchor");
	}

	#[test]
	fn append_reports_no_operator_sample() {
		// Append's mappings live in the shared row-number registry, so a mapping leak here is
		// attributed through the registry's per-operator metrics, not a per-operator sample.
		assert!(HostOperator::sample(&op(11)).is_none(), "append has no owned operator state to sample");
	}
}
