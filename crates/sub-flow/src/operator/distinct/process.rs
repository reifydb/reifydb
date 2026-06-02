// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	encoded::key::EncodedKey,
	interface::change::Diff,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_engine::expression::context::EvalContext;
use reifydb_runtime::hash::{Hash128, xxh3_128};
use reifydb_value::{
	Result,
	value::{identity::IdentityId, row_number::RowNumber},
};

use crate::{
	operator::distinct::{
		operator::{DistinctOperator, EMPTY_PARAMS, EMPTY_SYMBOL_TABLE},
		state::{DistinctEntry, DistinctState, SerializedRow},
	},
	transaction::FlowTransaction,
};

impl DistinctOperator {
	pub(super) fn slot_key(hash: Hash128) -> EncodedKey {
		let mut s = KeySerializer::new();
		s.extend_bytes(hash.0.to_be_bytes());
		s.finish()
	}

	pub(super) fn with_stable_rn(cols: Columns, stable_rn: RowNumber) -> Columns {
		Columns::with_system_columns(
			cols.iter().map(|c| ColumnWithName::new(c.name().clone(), c.data().clone())).collect(),
			vec![stable_rn],
			cols.created_at.to_vec(),
			cols.updated_at.to_vec(),
		)
	}

	pub(super) fn compute_hashes(&self, columns: &Columns) -> Result<Vec<Hash128>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.compiled_expressions.is_empty() {
			let mut hashes = Vec::with_capacity(row_count);
			for row_idx in 0..row_count {
				let mut data = Vec::new();
				for col in columns.iter() {
					let value = col.data().get_value(row_idx);
					let value_str = value.to_string();
					data.extend_from_slice(value_str.as_bytes());
				}
				hashes.push(xxh3_128(&data));
			}
			Ok(hashes)
		} else {
			let session = EvalContext {
				params: &EMPTY_PARAMS,
				symbols: &EMPTY_SYMBOL_TABLE,
				routines: &self.routines,
				runtime_context: &self.runtime_context,
				arena: None,
				identity: IdentityId::root(),
				is_aggregate_context: false,
				columns: Columns::empty(),
				row_count: 1,
				target: None,
				take: None,
			};
			let exec_ctx = session.with_eval(columns.clone(), row_count);
			let mut expr_columns = Vec::new();
			for compiled_expr in &self.compiled_expressions {
				let col = compiled_expr.execute(&exec_ctx)?;
				expr_columns.push(col);
			}

			let mut hashes = Vec::with_capacity(row_count);
			for row_idx in 0..row_count {
				let mut data = Vec::new();
				for col in &expr_columns {
					let value = col.data().get_value(row_idx);
					let value_str = value.to_string();
					data.extend_from_slice(value_str.as_bytes());
				}
				hashes.push(xxh3_128(&data));
			}
			Ok(hashes)
		}
	}

	pub(super) fn process_insert(
		&self,
		txn: &mut FlowTransaction,
		state: &mut DistinctState,
		columns: &Columns,
	) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		state.layout.update_from_columns(columns);
		let hashes = self.compute_hashes(columns)?;
		let now_nanos = self.runtime_context.clock.now_nanos();

		let mut order: Vec<usize> = (0..row_count).collect();
		if !columns.row_numbers.is_empty() {
			order.sort_by(|&a, &b| columns.row_numbers[b].cmp(&columns.row_numbers[a]));
		}

		let mut new_entries: Vec<(usize, Hash128)> = Vec::new();
		let mut swap_pairs: Vec<(SerializedRow, usize, Hash128)> = Vec::new();

		for &row_idx in &order {
			let hash = hashes[row_idx];
			let row_number = columns.row_numbers[row_idx];
			let new_serialized = SerializedRow::from_columns_at_index(columns, row_idx);

			if let Some(entry) = state.entries.get_mut(&hash) {
				entry.last_seen_nanos = now_nanos;
				let prev_rn = entry.rows.keys().next_back().copied().unwrap();
				let displaced = if row_number > prev_rn {
					entry.rows.get(&prev_rn).cloned()
				} else {
					None
				};
				entry.rows.insert(row_number, new_serialized);
				if let Some(prev) = displaced {
					swap_pairs.push((prev, row_idx, hash));
				}
			} else {
				let mut rows = BTreeMap::new();
				rows.insert(row_number, new_serialized);
				state.entries.insert(
					hash,
					DistinctEntry {
						rows,
						last_seen_nanos: now_nanos,
					},
				);
				new_entries.push((row_idx, hash));
			}
		}

		new_entries.sort_by_key(|&(i, _)| columns.row_numbers[i]);
		swap_pairs.sort_by_key(|&(_, i, _)| columns.row_numbers[i]);

		if !new_entries.is_empty() {
			let indices: Vec<usize> = new_entries.iter().map(|&(i, _)| i).collect();
			let mut stable_rns: Vec<RowNumber> = Vec::with_capacity(new_entries.len());
			for &(_, hash) in &new_entries {
				let (stable_rn, _) = self
					.row_number_provider
					.get_or_create_row_number(txn, &Self::slot_key(hash))?;
				stable_rns.push(stable_rn);
			}
			let source_cols = columns.extract_by_indices(&indices);
			let output = Columns::with_system_columns(
				source_cols
					.iter()
					.map(|c| ColumnWithName::new(c.name().clone(), c.data().clone()))
					.collect(),
				stable_rns,
				source_cols.created_at.to_vec(),
				source_cols.updated_at.to_vec(),
			);
			result.push(Diff::insert(output));
		}

		for (old_serialized, new_idx, hash) in swap_pairs {
			let (stable_rn, _) =
				self.row_number_provider.get_or_create_row_number(txn, &Self::slot_key(hash))?;
			let pre_cols = Self::with_stable_rn(old_serialized.to_columns(&state.layout), stable_rn);
			let post_cols = Self::with_stable_rn(columns.extract_by_indices(&[new_idx]), stable_rn);
			result.push(Diff::update(pre_cols, post_cols));
		}

		Ok(result)
	}

	pub(super) fn process_update(
		&self,
		txn: &mut FlowTransaction,
		state: &mut DistinctState,
		pre_columns: &Columns,
		post_columns: &Columns,
	) -> Result<Vec<Diff>> {
		let row_count = post_columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		state.layout.update_from_columns(post_columns);
		let pre_hashes = self.compute_hashes(pre_columns)?;
		let post_hashes = self.compute_hashes(post_columns)?;
		let now_nanos = self.runtime_context.clock.now_nanos();

		let mut result = Vec::new();

		for row_idx in 0..row_count {
			let pre_hash = pre_hashes[row_idx];
			let post_hash = post_hashes[row_idx];
			let row_number = post_columns.row_numbers[row_idx];

			if pre_hash == post_hash {
				let new_serialized = SerializedRow::from_columns_at_index(post_columns, row_idx);
				let visible = if let Some(entry) = state.entries.get_mut(&pre_hash) {
					entry.last_seen_nanos = now_nanos;
					let visible_rn = entry.rows.keys().next_back().copied();
					entry.rows.insert(row_number, new_serialized);
					visible_rn == Some(row_number)
				} else {
					false
				};
				if visible {
					let (stable_rn, _) = self
						.row_number_provider
						.get_or_create_row_number(txn, &Self::slot_key(pre_hash))?;
					let pre_out = Self::with_stable_rn(
						pre_columns.extract_by_indices(&[row_idx]),
						stable_rn,
					);
					let post_out = Self::with_stable_rn(
						post_columns.extract_by_indices(&[row_idx]),
						stable_rn,
					);
					result.push(Diff::update(pre_out, post_out));
				}
				continue;
			}

			let pre_mutation: Option<(bool, Option<SerializedRow>)> = {
				if let Some(entry) = state.entries.get_mut(&pre_hash) {
					let prev_rn = entry.rows.keys().next_back().copied().unwrap();
					let removed = entry.rows.remove(&row_number).is_some();
					if removed {
						if entry.rows.is_empty() {
							Some((true, None))
						} else {
							let new_rn = entry.rows.keys().next_back().copied().unwrap();
							if new_rn != prev_rn {
								let new_visible =
									entry.rows.get(&new_rn).cloned().unwrap();
								Some((false, Some(new_visible)))
							} else {
								None
							}
						}
					} else {
						None
					}
				} else {
					None
				}
			};

			if state.entries.get(&pre_hash).map(|e| e.rows.is_empty()).unwrap_or(false) {
				state.entries.shift_remove(&pre_hash);
			}

			let new_serialized = SerializedRow::from_columns_at_index(post_columns, row_idx);
			let post_mutation: (bool, Option<SerializedRow>) =
				if let Some(entry) = state.entries.get_mut(&post_hash) {
					entry.last_seen_nanos = now_nanos;
					let prev_rn = entry.rows.keys().next_back().copied().unwrap();
					let displaced = if row_number > prev_rn {
						entry.rows.get(&prev_rn).cloned()
					} else {
						None
					};
					entry.rows.insert(row_number, new_serialized);
					(false, displaced)
				} else {
					let mut rows = BTreeMap::new();
					rows.insert(row_number, new_serialized);
					state.entries.insert(
						post_hash,
						DistinctEntry {
							rows,
							last_seen_nanos: now_nanos,
						},
					);
					(true, None)
				};

			if let Some((pre_is_empty, pre_new_visible_opt)) = pre_mutation {
				let (stable_rn, _) = self
					.row_number_provider
					.get_or_create_row_number(txn, &Self::slot_key(pre_hash))?;
				if pre_is_empty {
					self.row_number_provider
						.remove_by_prefix(txn, Self::slot_key(pre_hash).as_ref())?;
					result.push(Diff::remove(Self::with_stable_rn(
						pre_columns.extract_by_indices(&[row_idx]),
						stable_rn,
					)));
				} else if let Some(new_visible) = pre_new_visible_opt {
					result.push(Diff::update(
						Self::with_stable_rn(
							pre_columns.extract_by_indices(&[row_idx]),
							stable_rn,
						),
						Self::with_stable_rn(new_visible.to_columns(&state.layout), stable_rn),
					));
				}
			}

			let (post_is_new, post_displaced_opt) = post_mutation;
			if post_is_new || post_displaced_opt.is_some() {
				let (stable_rn, _) = self
					.row_number_provider
					.get_or_create_row_number(txn, &Self::slot_key(post_hash))?;
				if let Some(old_visible) = post_displaced_opt {
					result.push(Diff::update(
						Self::with_stable_rn(old_visible.to_columns(&state.layout), stable_rn),
						Self::with_stable_rn(
							post_columns.extract_by_indices(&[row_idx]),
							stable_rn,
						),
					));
				} else {
					result.push(Diff::insert(Self::with_stable_rn(
						post_columns.extract_by_indices(&[row_idx]),
						stable_rn,
					)));
				}
			}
		}

		Ok(result)
	}

	pub(super) fn process_remove(
		&self,
		txn: &mut FlowTransaction,
		state: &mut DistinctState,
		columns: &Columns,
	) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		let hashes = self.compute_hashes(columns)?;

		let mut mutations: Vec<(usize, Hash128, Option<Option<SerializedRow>>)> = Vec::new();
		let mut empty_hashes: Vec<Hash128> = Vec::new();

		for (row_idx, &hash) in hashes.iter().enumerate() {
			let row_number = columns.row_numbers[row_idx];

			let Some(entry) = state.entries.get_mut(&hash) else {
				continue;
			};

			let prev_rn = entry.rows.keys().next_back().copied().unwrap();
			let removed = entry.rows.remove(&row_number).is_some();
			if !removed {
				continue;
			}

			if entry.rows.is_empty() {
				empty_hashes.push(hash);
				mutations.push((row_idx, hash, Some(None)));
				continue;
			}

			let new_rn = entry.rows.keys().next_back().copied().unwrap();
			if new_rn != prev_rn {
				let new_visible = entry.rows.get(&new_rn).cloned().unwrap();
				mutations.push((row_idx, hash, Some(Some(new_visible))));
			} else {
				mutations.push((row_idx, hash, None));
			}
		}

		for hash in empty_hashes {
			state.entries.shift_remove(&hash);
		}

		for (row_idx, hash, mutation) in mutations {
			let Some(new_visible_opt) = mutation else {
				continue;
			};
			let (stable_rn, _) =
				self.row_number_provider.get_or_create_row_number(txn, &Self::slot_key(hash))?;
			match new_visible_opt {
				None => {
					self.row_number_provider
						.remove_by_prefix(txn, Self::slot_key(hash).as_ref())?;
					result.push(Diff::remove(Self::with_stable_rn(
						columns.extract_by_indices(&[row_idx]),
						stable_rn,
					)));
				}
				Some(new_visible) => {
					result.push(Diff::update(
						Self::with_stable_rn(columns.extract_by_indices(&[row_idx]), stable_rn),
						Self::with_stable_rn(new_visible.to_columns(&state.layout), stable_rn),
					));
				}
			}
		}

		Ok(result)
	}
}
