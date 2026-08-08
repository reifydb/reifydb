// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::{BTreeMap, HashMap},
	mem::size_of,
	slice::from_ref,
};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	encoded::{
		bytes::EncodedBytes,
		shape::{RowShape, RowShapeField},
	},
	operator::{decode, encode_archive},
};
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	metrics::heap::HeapSize,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::Operator,
	transaction::{FlowTransaction, slot::PersistFn},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	error::Error,
	value::{Value, datetime::DateTime, row_number::RowNumber},
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
	error::FlowStateError,
	operator::{
		OperatorCell,
		stateful::{raw::RawStatefulOperator, utils},
	},
};

#[operator_state]
#[derive(Debug, Clone, Serialize, Deserialize, Default, HeapSize)]
struct TakeState {
	by_seq: BTreeMap<u64, RowNumber>,
	by_row: HashMap<RowNumber, (u64, usize)>,
	candidates_by_seq: BTreeMap<u64, RowNumber>,
	candidates_by_row: HashMap<RowNumber, (u64, usize)>,
	next_seq: u64,
	row_data: HashMap<RowNumber, EncodedBytes>,
}

pub struct TakeOperator {
	parent: OperatorCell,
	operator: OperatorId,
	limit: usize,
}

fn row_shape_from_columns(cols: &Columns) -> RowShape {
	let fields: Vec<RowShapeField> = cols
		.names
		.iter()
		.zip(cols.columns.iter())
		.map(|(name, buf)| RowShapeField::unconstrained(name.text().to_string(), buf.get_type()))
		.collect();
	RowShape::new(fields)
}

fn encode_take_bytes(shape: &RowShape, columns: &Columns, row_idx: usize) -> EncodedBytes {
	let values: Vec<Value> = columns.columns.iter().map(|buf| buf.get_value(row_idx)).collect();
	let mut encoded = shape.allocate();
	shape.set_values(&mut encoded, &values);
	encoded.freeze()
}

fn decode_take_bytes(shape: &RowShape, row_number: RowNumber, encoded: &EncodedBytes) -> Columns {
	Columns::from_encoded_bytes(shape, &[row_number], from_ref(encoded))
}

impl TakeOperator {
	pub fn new(parent: OperatorCell, operator: OperatorId, limit: usize) -> Self {
		Self {
			parent,
			operator,
			limit,
		}
	}

	fn load_take_state(&self, txn: &mut FlowTransaction) -> Result<TakeState> {
		let key = utils::empty_state_key();
		let Some(row) = utils::state_get(self.operator, txn, &key)? else {
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

	#[inline]
	fn take_state_usage(value: &dyn Any) -> ByteSize {
		let state = value.downcast_ref::<TakeState>().expect("TakeState slot type");
		ByteSize::from_bytes((size_of::<TakeState>() + state.heap_size()) as u64)
	}

	#[inline]
	fn acquire_take_state(&self, txn: &mut FlowTransaction) -> Result<(TakeState, PersistFn)> {
		let operator_id = self.operator;
		txn.take_operator_state::<TakeState, _>(operator_id, |txn| {
			let s = self.load_take_state(txn)?;
			let persist: PersistFn = Box::new(move |txn, value| {
				let state = value.downcast::<TakeState>().expect("TakeState slot type");
				let row = encode_archive(&*state, DateTime::MAX).map_err(|e| {
					Error::from(FlowStateError::Encode {
						state: "TakeState",
						cause: e.to_string(),
					})
				})?;
				utils::state_set(operator_id, txn, &utils::empty_state_key(), row)?;
				Ok(())
			});
			Ok((s, persist))
		})
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}

	#[inline]
	fn prune_candidates(&self, state: &mut TakeState) {
		let cap = self.limit.saturating_mul(4);
		while state.candidates_by_seq.len() > cap {
			let Some((&oldest_seq, &oldest_row)) = state.candidates_by_seq.iter().next() else {
				break;
			};
			state.candidates_by_seq.remove(&oldest_seq);
			state.candidates_by_row.remove(&oldest_row);
			state.row_data.remove(&oldest_row);
		}
	}

	#[inline]
	fn promote_one_candidate(&self, state: &mut TakeState, schema: &RowShape, output_diffs: &mut Vec<Diff>) {
		let Some((&seq, &row_number)) = state.candidates_by_seq.iter().next_back() else {
			return;
		};
		let count = state.candidates_by_row.get(&row_number).map(|(_, c)| *c).unwrap_or(1);
		state.candidates_by_seq.remove(&seq);
		state.candidates_by_row.remove(&row_number);
		state.by_seq.insert(seq, row_number);
		state.by_row.insert(row_number, (seq, count));

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

		let seq = state.next_seq;
		state.next_seq += 1;
		state.row_data.insert(row_number, encode_take_bytes(schema, &single_row, 0));
		state.by_seq.insert(seq, row_number);
		state.by_row.insert(row_number, (seq, 1));
		output_diffs.push(Diff::insert(single_row));

		if state.by_seq.len() > self.limit {
			let oldest = state.by_seq.iter().next().map(|(s, r)| (*s, *r));
			if let Some((oldest_seq, oldest_row)) = oldest {
				let count = state.by_row.get(&oldest_row).map(|(_, c)| *c).unwrap_or(1);
				state.by_seq.remove(&oldest_seq);
				state.by_row.remove(&oldest_row);
				state.candidates_by_seq.insert(oldest_seq, oldest_row);
				state.candidates_by_row.insert(oldest_row, (oldest_seq, count));
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
				let seq = slot.0;
				state.by_row.remove(&row_number);
				state.by_seq.remove(&seq);
				state.row_data.remove(&row_number);
				output_diffs.push(Diff::remove(pre.extract_by_indices(&[row_idx])));

				if state.by_seq.len() < self.limit && !state.candidates_by_seq.is_empty() {
					self.promote_one_candidate(state, &schema, output_diffs);
				}
				continue;
			}

			if let Some(slot) = state.candidates_by_row.get_mut(&row_number) {
				if slot.1 > 1 {
					slot.1 -= 1;
				} else {
					let seq = slot.0;
					state.candidates_by_row.remove(&row_number);
					state.candidates_by_seq.remove(&seq);
					state.row_data.remove(&row_number);
				}
			}
		}
	}
}

impl RawStatefulOperator for TakeOperator {}

impl Operator for TakeOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let operator_id = self.operator;
		let (mut state, persist) = self.acquire_take_state(txn)?;

		let mut output_diffs = Vec::new();
		let version = change.version;

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_insert_diff(&mut state, post, &mut output_diffs),
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_update_diff(&mut state, pre, post, &mut output_diffs),
				Diff::Remove {
					pre,
					..
				} => self.apply_remove_diff(&mut state, pre, &mut output_diffs),
			}
		}

		txn.put_operator_state(operator_id, state, persist, Self::take_state_usage);

		Ok(Change::from_flow(self.operator, version, output_diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}
