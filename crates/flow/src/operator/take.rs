// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	slice::from_ref,
};

use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	operator::{decode, encode},
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
	value::{Value, datetime::DateTime, row_number::RowNumber},
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
	RowShape::new(RowFamily::Operator, fields)
}

fn encode_take_bytes(shape: &RowShape, columns: &Columns, row_idx: usize) -> EncodedBytes {
	let values: Vec<Value> = columns.columns.iter().map(|buf| buf.get_value(row_idx)).collect();
	let mut encoded = shape.allocate_operator();
	shape.set_values(&mut encoded, &values);
	encoded.set_timestamps(
		columns.created_at().get(row_idx).copied().unwrap_or_default(),
		columns.updated_at().get(row_idx).copied().unwrap_or_default(),
	);
	if let Some(time) = columns.time().get(row_idx).copied() {
		encoded.set_time(time);
	}
	encoded.freeze_bytes()
}

fn decode_take_bytes(shape: &RowShape, row_number: RowNumber, encoded: &EncodedBytes) -> Columns {
	Columns::from_encoded_bytes(shape, &[row_number], from_ref(encoded))
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
		let row = encode(state, DateTime::MAX).map_err(|e| {
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
	fn promote_one_candidate(&self, state: &mut TakeState, schema: &RowShape, output_diffs: &mut Vec<Diff>) {
		let Some((&age, &row_number)) = state.candidates_by_age.iter().next_back() else {
			return;
		};
		let count = state.candidates_by_row.get(&row_number).map(|(_, c)| *c).unwrap_or(1);
		state.candidates_by_age.remove(&age);
		state.candidates_by_row.remove(&row_number);
		state.by_age.insert(age, row_number);
		state.by_row.insert(row_number, (age, count));

		if let Some(encoded) = state.row_data.get(&row_number) {
			let cols = decode_take_bytes(schema, row_number, encoded);
			if !cols.is_empty() {
				output_diffs.push(Diff::insert(cols));
			}
		}
	}

	#[inline]
	fn admit_new_row(
		&self,
		state: &mut TakeState,
		row_number: RowNumber,
		single_row: Columns,
		schema: &RowShape,
		output_diffs: &mut Vec<Diff>,
	) {
		if self.limit == 0 {
			return;
		}

		let age = RowAge::of(&single_row, 0, row_number);
		state.row_data.insert(row_number, encode_take_bytes(schema, &single_row, 0));

		if state.by_age.len() >= self.limit
			&& state.by_age.keys().next().is_some_and(|oldest_live| age <= *oldest_live)
		{
			state.candidates_by_age.insert(age, row_number);
			state.candidates_by_row.insert(row_number, (age, 1));
			self.prune_candidates(state);
			return;
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
					let cols = decode_take_bytes(schema, oldest_row, encoded);
					if !cols.is_empty() {
						output_diffs.push(Diff::remove(cols));
					}
				}
			}
		}

		self.prune_candidates(state);
	}

	#[inline]
	#[instrument(name = "flow::operator::take::insert", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_insert_diff(&self, state: &mut TakeState, post: Columns, output_diffs: &mut Vec<Diff>) {
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
			self.admit_new_row(state, row_number, single, &schema, output_diffs);
		}
	}

	#[inline]
	#[instrument(name = "flow::operator::take::update", level = "trace", skip_all, fields(rows = post.row_count()))]
	fn apply_update_diff(&self, state: &mut TakeState, pre: Columns, post: Columns, output_diffs: &mut Vec<Diff>) {
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
			self.admit_new_row(state, row_number, single, &schema, output_diffs);
		}

		if !update_indices.is_empty() {
			output_diffs.push(Diff::update(
				pre.extract_by_indices(&update_indices),
				post.extract_by_indices(&update_indices),
			));
		}
	}

	#[inline]
	#[instrument(name = "flow::operator::take::remove", level = "trace", skip_all, fields(rows = pre.row_count()))]
	fn apply_remove_diff(&self, state: &mut TakeState, pre: Columns, output_diffs: &mut Vec<Diff>) {
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
					self.promote_one_candidate(state, &schema, output_diffs);
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
				} => self.plan.apply_insert_diff(&mut state, post, &mut output_diffs),
				Diff::Update {
					pre,
					post,
					..
				} => self.plan.apply_update_diff(&mut state, pre, post, &mut output_diffs),
				Diff::Remove {
					pre,
					..
				} => self.plan.apply_remove_diff(&mut state, pre, &mut output_diffs),
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
}
