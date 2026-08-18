// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	slice::from_ref,
};

use reifydb_codec::row::{
	bytes::EncodedBytes,
	envelope::{Envelope, EnvelopeBuilder},
	pod::{
		EncodedPodRow,
		state::{decode, encode},
	},
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
	metrics::heap::HeapSize,
	value::column::columns::Columns,
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns},
};
use tracing::instrument;

use crate::{
	error::FlowStateError,
	operator::{HostOperator, host::HostContext, state::store},
};

#[operator_state]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, HeapSize)]
struct RowAge {
	created_at: DateTime,
	row: RowNumber,
}

impl RowAge {
	fn of(columns: &Columns, row_idx: usize, row: RowNumber) -> Self {
		let created_at = columns.created_at().get(row_idx).copied();
		reifydb_assertions! {
			assert!(
				created_at.is_some(),
				"row {:?} reached take without a created_at, so it ties with every other stampless row at \
				 the epoch and the window silently orders by row number alone",
				row
			);
		}
		Self {
			created_at: created_at.unwrap_or_default(),
			row,
		}
	}
}

#[operator_state]
#[derive(Debug, Clone, Default, HeapSize)]
struct TakeState {
	by_age: BTreeMap<RowAge, RowNumber>,
	by_row: HashMap<RowNumber, (RowAge, usize)>,
	candidates_by_age: BTreeMap<RowAge, RowNumber>,
	candidates_by_row: HashMap<RowNumber, (RowAge, usize)>,
	row_data: HashMap<RowNumber, EncodedBytes>,
}

pub struct TakePlan {
	parent_schema: Option<Columns>,
	operator: OperatorId,
	limit: usize,
}

pub struct TakeOperator {
	plan: TakePlan,
}

fn row_shape_from_columns(cols: &Columns) -> RowShape {
	let fields: Vec<RowShapeField> = cols
		.names
		.iter()
		.zip(cols.columns.iter())
		.map(|(name, buf)| RowShapeField::unconstrained(name.text().to_string(), buf.get_type()))
		.collect();
	RowShape::new(RowFamily::Pod, fields)
}

fn encode_take_bytes(shape: &RowShape, columns: &Columns, row_idx: usize) -> EncodedBytes {
	let values: Vec<Value> = columns.columns.iter().map(|buf| buf.get_value(row_idx)).collect();
	let mut encoded = shape.allocate_pod();
	shape.set_values(&mut encoded, &values);
	let body = encoded.freeze();

	let mut envelope = EnvelopeBuilder::new()
		.created_at(columns.created_at().get(row_idx).copied().unwrap_or_default())
		.updated_at(columns.updated_at().get(row_idx).copied().unwrap_or_default());
	if let Some(time) = columns.time().get(row_idx).copied() {
		envelope = envelope.time(time);
	}
	envelope.build(body.as_slice()).into_bytes()
}

fn decode_take_bytes(shape: &RowShape, row_number: RowNumber, encoded: &EncodedBytes) -> Result<Columns> {
	let envelope = Envelope::try_view(EncodedPodRow::view(encoded))?;
	let body = EncodedBytes(CowVec::new(envelope.body().to_vec()));

	let mut decoded = Columns::from_encoded_bytes(shape, &[row_number], from_ref(&body));
	decoded.system = SystemColumns::new(
		vec![row_number],
		Vec::new(),
		vec![envelope.created_at().unwrap_or_default()],
		vec![envelope.updated_at().unwrap_or_default()],
		envelope.time().into_iter().collect(),
	);
	Ok(decoded)
}

impl TakeOperator {
	pub fn new(parent_schema: Option<Columns>, operator: OperatorId, limit: usize) -> Self {
		Self {
			plan: TakePlan {
				parent_schema,
				operator,
				limit,
			},
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.plan.parent_schema.clone()
	}
}

impl TakePlan {
	fn state_key() -> GroupStateKey {
		OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::CUSTOM, b"")
	}

	fn load_take_state(&self, host: &mut dyn HostContext) -> Result<TakeState> {
		let key = Self::state_key();
		let Some(row) = store::state_get(host, &key)? else {
			return Ok(TakeState::default());
		};
		if row.is_empty() {
			return Ok(TakeState::default());
		}
		decode::<TakeState>(&row).map_err(|e| {
			Error::from(FlowStateError::Decode {
				state: "TakeState",
				cause: e.to_string(),
			})
		})
	}

	fn store_take_state(&self, host: &mut dyn HostContext, state: &TakeState) -> Result<()> {
		let row = encode(state).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "TakeState",
				cause: e.to_string(),
			})
		})?;
		store::state_set(host, &Self::state_key(), row)
	}

	#[inline]
	fn prune_candidates(&self, state: &mut TakeState) {
		let cap = self.limit.saturating_mul(4);
		while state.candidates_by_age.len() > cap {
			let Some((&oldest_age, &oldest_row)) = state.candidates_by_age.iter().next() else {
				break;
			};
			state.candidates_by_age.remove(&oldest_age);
			state.candidates_by_row.remove(&oldest_row);
			state.row_data.remove(&oldest_row);
		}
	}

	#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
	fn assert_candidates_stay_older_than_live(&self, state: &TakeState) {
		reifydb_assertions! {
			if let (Some(newest_candidate), Some(oldest_live)) =
				(state.candidates_by_age.keys().next_back(), state.by_age.keys().next())
			{
				assert!(
					newest_candidate < oldest_live,
					"a demoted candidate is at least as new as the oldest live row, so the next removal \
					 promotes a row the subscriber should already hold and skips the one that should \
					 enter (candidate={:?}, live={:?})",
					newest_candidate,
					oldest_live
				);
			}
		}
	}

	#[inline]
	fn promote_one_candidate(
		&self,
		state: &mut TakeState,
		schema: &RowShape,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let Some((&age, &row_number)) = state.candidates_by_age.iter().next_back() else {
			return Ok(());
		};
		let count = state.candidates_by_row.get(&row_number).map(|(_, c)| *c).unwrap_or(1);
		state.candidates_by_age.remove(&age);
		state.candidates_by_row.remove(&row_number);
		state.by_age.insert(age, row_number);
		state.by_row.insert(row_number, (age, count));

		if let Some(encoded) = state.row_data.get(&row_number) {
			let cols = decode_take_bytes(schema, row_number, encoded)?;
			if !cols.is_empty() {
				output_diffs.push(Diff::insert(cols));
			}
		}
		Ok(())
	}

	#[inline]
	fn admit_new_row(
		&self,
		state: &mut TakeState,
		row_number: RowNumber,
		single_row: Columns,
		schema: &RowShape,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		if self.limit == 0 {
			return Ok(());
		}

		let age = RowAge::of(&single_row, 0, row_number);
		state.row_data.insert(row_number, encode_take_bytes(schema, &single_row, 0));

		if state.by_age.len() >= self.limit
			&& state.by_age.keys().next().is_some_and(|oldest_live| age <= *oldest_live)
		{
			state.candidates_by_age.insert(age, row_number);
			state.candidates_by_row.insert(row_number, (age, 1));
			self.prune_candidates(state);
			return Ok(());
		}

		state.by_age.insert(age, row_number);
		state.by_row.insert(row_number, (age, 1));
		output_diffs.push(Diff::insert(single_row));

		if state.by_age.len() > self.limit {
			let oldest = state.by_age.iter().next().map(|(a, r)| (*a, *r));
			if let Some((oldest_age, oldest_row)) = oldest {
				let count = state.by_row.get(&oldest_row).map(|(_, c)| *c).unwrap_or(1);
				state.by_age.remove(&oldest_age);
				state.by_row.remove(&oldest_row);
				state.candidates_by_age.insert(oldest_age, oldest_row);
				state.candidates_by_row.insert(oldest_row, (oldest_age, count));
				if let Some(encoded) = state.row_data.get(&oldest_row) {
					let cols = decode_take_bytes(schema, oldest_row, encoded)?;
					if !cols.is_empty() {
						output_diffs.push(Diff::remove(cols));
					}
				}
			}
		}

		self.prune_candidates(state);
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::take::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_insert_diff(&self, state: &mut TakeState, post: Columns, output_diffs: &mut Vec<Diff>) -> Result<()> {
		let schema = row_shape_from_columns(&post);
		let row_count = post.row_count();
		for row_idx in 0..row_count {
			let row_number = post.row_numbers()[row_idx];

			if let Some(slot) = state.by_row.get_mut(&row_number) {
				slot.1 += 1;
				continue;
			}

			if let Some(slot) = state.candidates_by_row.get_mut(&row_number) {
				slot.1 += 1;
				continue;
			}

			let single = post.extract_by_indices(&[row_idx]);
			self.admit_new_row(state, row_number, single, &schema, output_diffs)?;
		}
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::take::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_update_diff(
		&self,
		state: &mut TakeState,
		pre: Columns,
		post: Columns,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let schema = row_shape_from_columns(&post);
		let row_count = post.row_count();
		let mut update_indices: Vec<usize> = Vec::new();

		for row_idx in 0..row_count {
			let row_number = post.row_numbers()[row_idx];

			if state.by_row.contains_key(&row_number) {
				update_indices.push(row_idx);
				state.row_data.insert(row_number, encode_take_bytes(&schema, &post, row_idx));
				continue;
			}

			if state.candidates_by_row.contains_key(&row_number) {
				state.row_data.insert(row_number, encode_take_bytes(&schema, &post, row_idx));
				continue;
			}

			let single = post.extract_by_indices(&[row_idx]);
			self.admit_new_row(state, row_number, single, &schema, output_diffs)?;
		}

		if !update_indices.is_empty() {
			output_diffs.push(Diff::update(
				pre.extract_by_indices(&update_indices),
				post.extract_by_indices(&update_indices),
			));
		}
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::operator::take::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn apply_remove_diff(&self, state: &mut TakeState, pre: Columns, output_diffs: &mut Vec<Diff>) -> Result<()> {
		let schema = row_shape_from_columns(&pre);
		let row_count = pre.row_count();
		for row_idx in 0..row_count {
			let row_number = pre.row_numbers()[row_idx];

			if let Some(slot) = state.by_row.get_mut(&row_number) {
				if slot.1 > 1 {
					slot.1 -= 1;
					continue;
				}
				let age = slot.0;
				state.by_row.remove(&row_number);
				state.by_age.remove(&age);
				state.row_data.remove(&row_number);
				output_diffs.push(Diff::remove(pre.extract_by_indices(&[row_idx])));

				if state.by_age.len() < self.limit && !state.candidates_by_age.is_empty() {
					self.promote_one_candidate(state, &schema, output_diffs)?;
				}
				continue;
			}

			if let Some(slot) = state.candidates_by_row.get_mut(&row_number) {
				if slot.1 > 1 {
					slot.1 -= 1;
				} else {
					let age = slot.0;
					state.candidates_by_row.remove(&row_number);
					state.candidates_by_age.remove(&age);
					state.row_data.remove(&row_number);
				}
			}
		}
		Ok(())
	}
}

impl HostOperator for TakeOperator {
	fn id(&self) -> OperatorId {
		self.plan.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let mut state = self.plan.load_take_state(host)?;

		let mut output_diffs = Vec::new();
		let version = change.version;

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.plan.apply_insert_diff(&mut state, post, &mut output_diffs)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.plan.apply_update_diff(&mut state, pre, post, &mut output_diffs)?,
				Diff::Remove {
					pre,
					..
				} => self.plan.apply_remove_diff(&mut state, pre, &mut output_diffs)?,
			}
		}

		self.plan.assert_candidates_stay_older_than_live(&state);
		self.plan.store_take_state(host, &state)?;

		Ok(Change::from_flow(self.plan.operator, version, output_diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		value::column::{ColumnWithName, buffer::ColumnBuffer},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{fragment::Fragment, value::system_columns::SystemColumns};

	use super::*;
	use crate::{
		operator::host::TxnHostContext,
		transaction::{deferred::DeferredTransaction, mock::FlowTxn},
	};

	fn row(n: i32, rn: u64, born_nanos: u64) -> Columns {
		// created_at is the age the operator must sort on, so every fixture sets it apart from arrival order.
		let at = DateTime::from_nanos(born_nanos);
		Columns::with_system(
			vec![ColumnWithName::new(Fragment::internal("n"), ColumnBuffer::int4(vec![n]))],
			SystemColumns::new(vec![RowNumber(rn)], Vec::new(), vec![at], vec![at], vec![at]),
		)
	}

	fn stamped_row(rn: u64, created: u64, updated: u64, time: Option<u64>) -> Columns {
		// the round trip is only lossless if each stamp lands in its own slot, so every fixture value differs.
		Columns::with_system(
			vec![ColumnWithName::new(Fragment::internal("n"), ColumnBuffer::int4(vec![rn as i32]))],
			SystemColumns::new(
				vec![RowNumber(rn)],
				Vec::new(),
				vec![DateTime::from_nanos(created)],
				vec![DateTime::from_nanos(updated)],
				time.map(DateTime::from_nanos).into_iter().collect(),
			),
		)
	}

	fn feed(op: &mut TakeOperator, txn: &mut DeferredTransaction, cols: Columns) -> Vec<Diff> {
		let operator = op.plan.operator;
		let change = Change::from_flow(
			operator,
			CommitVersion(1),
			vec![Diff::insert(cols)],
			DateTime::from_nanos(0),
		);
		op.apply(&mut TxnHostContext::new(txn, operator), change).unwrap().diffs.to_vec()
	}

	fn removed(diffs: &[Diff]) -> Vec<u64> {
		diffs.iter()
			.filter_map(|d| match d {
				Diff::Remove {
					pre,
					..
				} => Some(pre.row_numbers().iter().map(|r| r.0).collect::<Vec<_>>()),
				_ => None,
			})
			.flatten()
			.collect()
	}

	fn inserted(diffs: &[Diff]) -> Vec<u64> {
		diffs.iter()
			.filter_map(|d| match d {
				Diff::Insert {
					post,
					..
				} => Some(post.row_numbers().iter().map(|r| r.0).collect::<Vec<_>>()),
				_ => None,
			})
			.flatten()
			.collect()
	}

	#[test]
	fn a_row_older_than_the_whole_window_is_never_announced() {
		// Admitting a doomed late arrival and evicting it in the same change hands the subscriber an insert it
		// has to undo one diff later, so it must produce no diff at all.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let mut op = TakeOperator::new(None, OperatorId(1), 2);

		feed(&mut op, &mut txn, row(10, 10, 1_000));
		feed(&mut op, &mut txn, row(11, 11, 2_000));
		let diffs = feed(&mut op, &mut txn, row(7, 7, 500));

		assert!(diffs.is_empty(), "a row older than every live row must produce no diff, got {:?}", diffs);
	}

	#[test]
	fn a_newest_first_feed_never_evicts_a_row_it_just_admitted() {
		// This is the hydration order: rows arrive youngest first. Keying the window on arrival makes the
		// first row look oldest, so the third feed evicts the newest row instead of ignoring the oldest.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let mut op = TakeOperator::new(None, OperatorId(1), 2);

		let first = feed(&mut op, &mut txn, row(3, 3, 3_000));
		let second = feed(&mut op, &mut txn, row(2, 2, 2_000));
		let third = feed(&mut op, &mut txn, row(1, 1, 1_000));

		assert_eq!(inserted(&first), vec![3]);
		assert_eq!(inserted(&second), vec![2]);
		assert!(third.is_empty(), "the oldest row cannot enter a full window, got {:?}", third);
		assert!(
			removed(&first).is_empty() && removed(&second).is_empty() && removed(&third).is_empty(),
			"nothing may be evicted while the window is filling from newest to oldest"
		);
	}

	#[test]
	fn every_stamp_survives_the_take_row_round_trip_unchanged() {
		// The pod body carries no header, so every stamp must survive in the envelope or it is lost.
		let cols = stamped_row(7, 1_000, 2_000, Some(3_000));
		let shape = row_shape_from_columns(&cols);
		let encoded = encode_take_bytes(&shape, &cols, 0);
		let decoded = decode_take_bytes(&shape, RowNumber(7), &encoded).unwrap();

		assert_eq!(decoded.created_at(), &[DateTime::from_nanos(1_000)]);
		assert_eq!(decoded.updated_at(), &[DateTime::from_nanos(2_000)]);
		assert_eq!(decoded.time(), &[DateTime::from_nanos(3_000)]);
		assert_eq!(decoded.row_numbers(), &[RowNumber(7)]);
		assert_eq!(decoded[0].get_value(0), Value::Int4(7));
	}

	#[test]
	fn a_row_without_a_time_round_trips_with_time_absent_and_both_stamps_present() {
		// A timeless source must never gain a fabricated #time, and must still hand the sink a created_at.
		let cols = stamped_row(9, 4_000, 5_000, None);
		let shape = row_shape_from_columns(&cols);
		let encoded = encode_take_bytes(&shape, &cols, 0);
		let decoded = decode_take_bytes(&shape, RowNumber(9), &encoded).unwrap();

		assert!(
			decoded.time().is_empty(),
			"a source row with no #time must not gain one, got {:?}",
			decoded.time()
		);
		assert_eq!(decoded.created_at(), &[DateTime::from_nanos(4_000)]);
		assert_eq!(decoded.updated_at(), &[DateTime::from_nanos(5_000)]);
	}

	#[test]
	fn the_take_envelope_header_is_twenty_five_bytes_with_a_time_and_seventeen_without() {
		// The envelope must charge only for the fields set, otherwise a timeless row pays for a slot.
		let timed = stamped_row(1, 1_000, 2_000, Some(3_000));
		let timeless = stamped_row(2, 1_000, 2_000, None);
		let timed_bytes = encode_take_bytes(&row_shape_from_columns(&timed), &timed, 0);
		let timeless_bytes = encode_take_bytes(&row_shape_from_columns(&timeless), &timeless, 0);

		assert_eq!(Envelope::try_view(EncodedPodRow::view(&timed_bytes)).unwrap().header_size(), 25);
		assert_eq!(Envelope::try_view(EncodedPodRow::view(&timeless_bytes)).unwrap().header_size(), 17);
	}

	#[test]
	fn an_evicted_row_reaches_the_subscriber_with_its_stamps_intact() {
		// The remove diff is rebuilt from stored bytes, so only here does a lossy round trip surface.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let mut op = TakeOperator::new(None, OperatorId(1), 2);

		feed(&mut op, &mut txn, row(1, 1, 1_000));
		feed(&mut op, &mut txn, row(2, 2, 2_000));
		let diffs = feed(&mut op, &mut txn, row(3, 3, 3_000));

		assert_eq!(removed(&diffs), vec![1]);
		let Some(Diff::Remove {
			pre,
			..
		}) = diffs.iter().find(|d| matches!(d, Diff::Remove { .. }))
		else {
			panic!("admitting a newer row into a full window must evict the oldest one");
		};
		assert_eq!(pre.created_at(), &[DateTime::from_nanos(1_000)]);
		assert_eq!(pre.updated_at(), &[DateTime::from_nanos(1_000)]);
		assert_eq!(pre.time(), &[DateTime::from_nanos(1_000)]);
	}
}
