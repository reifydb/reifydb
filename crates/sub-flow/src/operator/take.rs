// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use postcard::{from_bytes, to_stdvec};
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
	active: BTreeMap<RowNumber, usize>,
	candidates: BTreeMap<RowNumber, usize>,
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

	fn save_take_state(&self, txn: &mut FlowTransaction, state: &TakeState) -> Result<()> {
		let serialized = to_stdvec(state)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize TakeState: {}", e))))?;
		let blob = Blob::from(serialized);

		self.update_state(txn, |shape, row| {
			shape.set_blob(row, 0, &blob);
			Ok(())
		})?;
		Ok(())
	}

	fn promote_candidates(&self, state: &mut TakeState, txn: &mut FlowTransaction) -> Result<Vec<Diff>> {
		let mut output_diffs = Vec::new();

		while state.active.len() < self.limit && !state.candidates.is_empty() {
			if let Some((&candidate_row, &count)) = state.candidates.iter().next_back() {
				state.candidates.remove(&candidate_row);
				state.active.insert(candidate_row, count);

				let cols = self.parent.pull(txn, &[candidate_row])?;
				if !cols.is_empty() {
					output_diffs.push(Diff::insert(cols));
				}
			}
		}

		Ok(output_diffs)
	}

	fn evict_to_candidates(&self, state: &mut TakeState, txn: &mut FlowTransaction) -> Result<Vec<Diff>> {
		let mut output_diffs = Vec::new();
		let candidate_limit = self.limit * 4;

		while state.active.len() > self.limit {
			if let Some((&evicted_row, &count)) = state.active.iter().next() {
				state.active.remove(&evicted_row);
				state.candidates.insert(evicted_row, count);

				let cols = self.parent.pull(txn, &[evicted_row])?;
				if !cols.is_empty() {
					output_diffs.push(Diff::remove(cols));
				}
			}
		}

		while state.candidates.len() > candidate_limit {
			if let Some((&removed_row, _)) = state.candidates.iter().next() {
				state.candidates.remove(&removed_row);
			}
		}

		Ok(output_diffs)
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
	fn admit_or_evict_new_row(
		&self,
		state: &mut TakeState,
		txn: &mut FlowTransaction,
		row_number: RowNumber,
		single_row: Columns,
		output_diffs: &mut Vec<Diff>,
	) -> Result<()> {
		if state.active.len() < self.limit {
			state.active.insert(row_number, 1);
			output_diffs.push(Diff::insert(single_row));
			return Ok(());
		}

		let Some(smallest) = state.active.keys().next().copied() else {
			return Ok(());
		};

		if row_number > smallest {
			if let Some(count) = state.active.remove(&smallest) {
				state.candidates.insert(smallest, count);
				let cols = self.parent.pull(txn, &[smallest])?;
				if !cols.is_empty() {
					output_diffs.push(Diff::remove(cols));
				}
			}
			state.active.insert(row_number, 1);
			output_diffs.push(Diff::insert(single_row));
		} else {
			state.candidates.insert(row_number, 1);
		}
		prune_candidates(state, self.limit);
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

			if state.active.contains_key(&row_number) {
				*state.active.get_mut(&row_number).unwrap() += 1;
				continue;
			}
			if state.candidates.contains_key(&row_number) {
				*state.candidates.get_mut(&row_number).unwrap() += 1;
				continue;
			}

			let single = post.extract_by_indices(&[row_idx]);
			self.admit_or_evict_new_row(state, txn, row_number, single, output_diffs)?;
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

			if state.active.contains_key(&row_number) {
				update_indices.push(row_idx);
				continue;
			}

			// Row suppressed by the take limit (kept as a candidate for
			// future promotion); subscriber is intentionally not receiving
			// it, so the Update is also suppressed.
			if state.candidates.contains_key(&row_number) {
				continue;
			}

			// Row is unknown to TakeState because it existed before the
			// subscription started (subscriptions have no backfill, so
			// TakeState begins empty even when the upstream view is
			// populated). The subscriber is seeing this row for the first
			// time, so emit the post-image as an Insert and run the same
			// admission/eviction policy that the Insert branch uses for
			// genuinely new rows. Without this, every Update against a
			// pre-existing row would be silently dropped.
			let single = post.extract_by_indices(&[row_idx]);
			self.admit_or_evict_new_row(state, txn, row_number, single, output_diffs)?;
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

			if let Some(count) = state.active.get_mut(&row_number) {
				if *count > 1 {
					*count -= 1;
				} else {
					state.active.remove(&row_number);
					output_diffs.push(Diff::remove(pre.extract_by_indices(&[row_idx])));
					let promoted = self.promote_candidates(state, txn)?;
					output_diffs.extend(promoted);
				}
			} else if let Some(count) = state.candidates.get_mut(&row_number) {
				if *count > 1 {
					*count -= 1;
				} else {
					state.candidates.remove(&row_number);
				}
			}
		}
		Ok(())
	}
}

#[inline]
fn prune_candidates(state: &mut TakeState, limit: usize) {
	let candidate_limit = limit * 4;
	while state.candidates.len() > candidate_limit {
		if let Some((&r, _)) = state.candidates.iter().next() {
			state.candidates.remove(&r);
		}
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

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let node_id = self.node;
		let (mut state, persist) = self.acquire_take_state(txn)?;

		let mut output_diffs = Vec::new();
		let version = change.version;

		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => self.apply_insert_diff(&mut state, txn, post, &mut output_diffs)?,
				Diff::Update {
					pre,
					post,
				} => self.apply_update_diff(&mut state, txn, pre, post, &mut output_diffs)?,
				Diff::Remove {
					pre,
				} => self.apply_remove_diff(&mut state, txn, pre, &mut output_diffs)?,
			}
		}

		// Restore the cached state for the next batch in this txn; the put
		// marks the slot dirty so flush_operator_states will persist it.
		txn.put_operator_state(node_id, state, persist);

		Ok(Change::from_flow(self.node, version, output_diffs, change.changed_at))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
