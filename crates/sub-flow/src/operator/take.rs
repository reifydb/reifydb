// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, rc::Rc};

use reifydb_core::{
	encoded::layout::EncodedValuesLayout, interface::catalog::flow::FlowNodeId, value::column::columns::Columns,
};
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
use reifydb_sdk::flow::{FlowChange, FlowDiff};
use reifydb_type::{
	error::Error,
	internal,
	value::{blob::Blob, row_number::RowNumber, r#type::Type},
};
use serde::{Deserialize, Serialize};

use crate::{
	operator::{
		Operator, Operators,
		stateful::{raw::RawStatefulOperator, single::SingleStateful},
	},
	transaction::FlowTransaction,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TakeState {
	active: BTreeMap<RowNumber, usize>,
	candidates: BTreeMap<RowNumber, usize>,
}

impl Default for TakeState {
	fn default() -> Self {
		Self {
			active: BTreeMap::new(),
			candidates: BTreeMap::new(),
		}
	}
}

pub struct TakeOperator {
	parent: Rc<Operators>,
	node: FlowNodeId,
	limit: usize,
	layout: EncodedValuesLayout,
}

impl TakeOperator {
	pub fn new(parent: Rc<Operators>, node: FlowNodeId, limit: usize) -> Self {
		Self {
			parent,
			node,
			limit,
			layout: EncodedValuesLayout::new(&[Type::Blob]),
		}
	}

	fn load_take_state(&self, txn: &mut FlowTransaction) -> reifydb_type::Result<TakeState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(TakeState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(TakeState::default());
		}

		postcard::from_bytes(blob.as_ref())
			.map_err(|e| Error(internal!("Failed to deserialize TakeState: {}", e)))
	}

	fn save_take_state(&self, txn: &mut FlowTransaction, state: &TakeState) -> reifydb_type::Result<()> {
		let serialized = postcard::to_stdvec(state)
			.map_err(|e| Error(internal!("Failed to serialize TakeState: {}", e)))?;

		let mut state_row = self.layout.allocate_deprecated();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, state_row)
	}

	fn promote_candidates(
		&self,
		state: &mut TakeState,
		txn: &mut FlowTransaction,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		let mut output_diffs = Vec::new();

		while state.active.len() < self.limit && !state.candidates.is_empty() {
			if let Some((&candidate_row, &count)) = state.candidates.iter().next_back() {
				state.candidates.remove(&candidate_row);
				state.active.insert(candidate_row, count);

				let cols = self.parent.pull(txn, &[candidate_row])?;
				if !cols.is_empty() {
					output_diffs.push(FlowDiff::Insert {
						post: cols,
					});
				}
			}
		}

		Ok(output_diffs)
	}

	fn evict_to_candidates(
		&self,
		state: &mut TakeState,
		txn: &mut FlowTransaction,
	) -> reifydb_type::Result<Vec<FlowDiff>> {
		let mut output_diffs = Vec::new();
		let candidate_limit = self.limit * 4;

		while state.active.len() > self.limit {
			if let Some((&evicted_row, &count)) = state.active.iter().next() {
				state.active.remove(&evicted_row);
				state.candidates.insert(evicted_row, count);

				let cols = self.parent.pull(txn, &[evicted_row])?;
				if !cols.is_empty() {
					output_diffs.push(FlowDiff::Remove {
						pre: cols,
					});
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
}

impl RawStatefulOperator for TakeOperator {}

impl SingleStateful for TakeOperator {
	fn layout(&self) -> EncodedValuesLayout {
		self.layout.clone()
	}
}

impl Operator for TakeOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
		let mut state = self.load_take_state(txn)?;
		let mut output_diffs = Vec::new();
		let version = change.version;

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let row_number = post.number();

					if state.active.contains_key(&row_number) {
						*state.active.get_mut(&row_number).unwrap() += 1;
						continue;
					}

					if state.candidates.contains_key(&row_number) {
						*state.candidates.get_mut(&row_number).unwrap() += 1;
						continue;
					}

					if state.active.len() < self.limit {
						state.active.insert(row_number, 1);
						output_diffs.push(FlowDiff::Insert {
							post,
						});
					} else {
						let smallest_active = state.active.keys().next().copied();

						if let Some(smallest) = smallest_active {
							if row_number > smallest {
								if let Some(count) = state.active.remove(&smallest) {
									state.candidates.insert(smallest, count);

									let cols =
										self.parent.pull(txn, &[smallest])?;

									if !cols.is_empty() {
										output_diffs.push(FlowDiff::Remove {
											pre: cols,
										});
									}
								}

								state.active.insert(row_number, 1);
								output_diffs.push(FlowDiff::Insert {
									post,
								});

								let candidate_limit = self.limit * 4;
								while state.candidates.len() > candidate_limit {
									if let Some((&removed_row, _)) =
										state.candidates.iter().next()
									{
										state.candidates.remove(&removed_row);
									}
								}
							} else {
								state.candidates.insert(row_number, 1);

								let candidate_limit = self.limit * 4;
								while state.candidates.len() > candidate_limit {
									if let Some((&removed_row, _)) =
										state.candidates.iter().next()
									{
										state.candidates.remove(&removed_row);
									}
								}
							}
						}
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					let row_number = post.number();

					if state.active.contains_key(&row_number) {
						output_diffs.push(FlowDiff::Update {
							pre,
							post,
						});
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					let row_number = pre.number();

					if let Some(count) = state.active.get_mut(&row_number) {
						if *count > 1 {
							*count -= 1;
						} else {
							state.active.remove(&row_number);
							output_diffs.push(FlowDiff::Remove {
								pre,
							});

							let promoted = self.promote_candidates(&mut state, txn)?;
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
			}
		}

		self.save_take_state(txn, &state)?;

		Ok(FlowChange::internal(self.node, version, output_diffs))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
