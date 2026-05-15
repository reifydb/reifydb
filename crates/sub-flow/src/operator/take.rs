// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	sync::Arc,
};

use postcard::{from_bytes, to_stdvec};
use reifydb_abi::operator::capabilities::CAPABILITY_ALL_STANDARD;
use reifydb_core::{
	encoded::shape::RowShape,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	internal,
	value::column::columns::Columns,
};
use reifydb_type::{
	Result,
	error::Error,
	value::{blob::Blob, row_number::RowNumber},
};
use serde::{Deserialize, Serialize};

use crate::{
	operator::{
		Operator, Operators,
		stateful::{raw::RawStatefulOperator, single::SingleStateful, utils},
	},
	transaction::{FlowTransaction, slot::PersistFn},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TakeState {
	by_seq: BTreeMap<u64, RowNumber>,
	by_row: HashMap<RowNumber, (u64, usize)>,
	candidates_by_seq: BTreeMap<u64, RowNumber>,
	candidates_by_row: HashMap<RowNumber, (u64, usize)>,
	next_seq: u64,
}

pub struct TakeOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	limit: usize,
	shape: RowShape,
}

impl TakeOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, limit: usize) -> Self {
		Self {
			parent,
			node,
			limit,
			shape: RowShape::operator_state(),
		}
	}

	fn load_take_state(&self, txn: &mut FlowTransaction) -> Result<TakeState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(TakeState::default());
		}

		let blob = self.shape.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(TakeState::default());
		}

		from_bytes(blob.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize TakeState: {}", e))))
	}

	#[inline]
	fn acquire_take_state(&self, txn: &mut FlowTransaction) -> Result<(TakeState, PersistFn)> {
		let node_id = self.node;
		let shape_for_persist = self.shape.clone();
		txn.take_operator_state::<TakeState, _>(node_id, |txn| {
			let s = self.load_take_state(txn)?;
			let shape = shape_for_persist.clone();
			let persist: PersistFn = Box::new(move |txn, value| {
				let state = value.downcast::<TakeState>().expect("TakeState slot type");
				let serialized = to_stdvec(&*state).map_err(|e| {
					Error(Box::new(internal!("Failed to serialize TakeState: {}", e)))
				})?;
				let blob = Blob::from(serialized);
				let key = utils::empty_key();
				let mut row = utils::load_or_create_row(node_id, txn, &key, &shape)?;
				shape.set_blob(&mut row, 0, &blob);
				utils::save_row(node_id, txn, &key, row)?;
				Ok(())
			});
			Ok((s, persist))
		})
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
		}
	}

	#[inline]
	fn promote_one_candidate(
		&self,
		state: &mut TakeState,
		txn: &mut FlowTransaction,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let Some((&seq, &row_number)) = state.candidates_by_seq.iter().next_back() else {
			return Ok(());
		};
		let count = state.candidates_by_row.get(&row_number).map(|(_, c)| *c).unwrap_or(1);
		state.candidates_by_seq.remove(&seq);
		state.candidates_by_row.remove(&row_number);
		state.by_seq.insert(seq, row_number);
		state.by_row.insert(row_number, (seq, count));

		let cols = self.parent.pull(txn, &[row_number])?;
		if !cols.is_empty() {
			output_diffs.push(Diff::insert(cols));
		}
		Ok(())
	}

	#[inline]
	fn admit_new_row(
		&self,
		state: &mut TakeState,
		txn: &mut FlowTransaction,
		row_number: RowNumber,
		single_row: Columns,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		if self.limit == 0 {
			return Ok(());
		}

		let seq = state.next_seq;
		state.next_seq += 1;
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
				let cols = self.parent.pull(txn, &[oldest_row])?;
				if !cols.is_empty() {
					output_diffs.push(Diff::remove(cols));
				}
			}
		}

		self.prune_candidates(state);
		Ok(())
	}

	#[inline]
	fn apply_insert_diff(
		&self,
		state: &mut TakeState,
		txn: &mut FlowTransaction,
		post: Arc<Columns>,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let row_count = post.row_count();
		for row_idx in 0..row_count {
			let row_number = post.row_numbers[row_idx];

			if let Some(slot) = state.by_row.get_mut(&row_number) {
				slot.1 += 1;
				continue;
			}

			if let Some(slot) = state.candidates_by_row.get_mut(&row_number) {
				slot.1 += 1;
				continue;
			}

			let single = post.extract_by_indices(&[row_idx]);
			self.admit_new_row(state, txn, row_number, single, output_diffs)?;
		}
		Ok(())
	}

	#[inline]
	fn apply_update_diff(
		&self,
		state: &mut TakeState,
		txn: &mut FlowTransaction,
		pre: Arc<Columns>,
		post: Arc<Columns>,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let row_count = post.row_count();
		let mut update_indices: Vec<usize> = Vec::new();

		for row_idx in 0..row_count {
			let row_number = post.row_numbers[row_idx];

			if state.by_row.contains_key(&row_number) {
				update_indices.push(row_idx);
				continue;
			}

			if state.candidates_by_row.contains_key(&row_number) {
				continue;
			}

			let single = post.extract_by_indices(&[row_idx]);
			self.admit_new_row(state, txn, row_number, single, output_diffs)?;
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
	fn apply_remove_diff(
		&self,
		state: &mut TakeState,
		txn: &mut FlowTransaction,
		pre: Arc<Columns>,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		let row_count = pre.row_count();
		for row_idx in 0..row_count {
			let row_number = pre.row_numbers[row_idx];

			if let Some(slot) = state.by_row.get_mut(&row_number) {
				if slot.1 > 1 {
					slot.1 -= 1;
					continue;
				}
				let seq = slot.0;
				state.by_row.remove(&row_number);
				state.by_seq.remove(&seq);
				output_diffs.push(Diff::remove(pre.extract_by_indices(&[row_idx])));

				if state.by_seq.len() < self.limit && !state.candidates_by_seq.is_empty() {
					self.promote_one_candidate(state, txn, output_diffs)?;
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
				}
			}
		}
		Ok(())
	}
}

impl RawStatefulOperator for TakeOperator {}

impl SingleStateful for TakeOperator {
	fn layout(&self) -> RowShape {
		self.shape.clone()
	}
}

impl Operator for TakeOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> u32 {
		CAPABILITY_ALL_STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let node_id = self.node;
		let (mut state, persist) = self.acquire_take_state(txn)?;

		let mut output_diffs = Vec::new();
		let version = change.version;

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_insert_diff(&mut state, txn, post, &mut output_diffs)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_update_diff(&mut state, txn, pre, post, &mut output_diffs)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_remove_diff(&mut state, txn, pre, &mut output_diffs)?,
			}
		}

		txn.put_operator_state(node_id, state, persist);

		Ok(Change::from_flow(self.node, version, output_diffs, change.changed_at))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
