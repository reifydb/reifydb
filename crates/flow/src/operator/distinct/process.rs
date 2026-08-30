// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_core::{
	interface::change::Diff,
	key::operator::state::GroupId,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_evaluate::expression::context::EvalContext;
use reifydb_value::{
	Result,
	util::hash::{Hash128, xxh3_128},
	value::{datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns},
};

use crate::operator::{
	distinct::{
		operator::DistinctPlan,
		state::{DistinctEntry, DistinctState, SerializedRow},
	},
	host::HostContext,
	state::store,
};

fn row_time(host: &dyn HostContext, columns: &Columns, row_idx: usize) -> DateTime {
	if columns.time().is_empty() {
		host.written_at()
	} else {
		columns.time()[row_idx]
	}
}

impl DistinctPlan {
	pub(super) fn with_stable_rn(cols: Columns, stable_rn: RowNumber) -> Columns {
		Columns::with_system(
			cols.iter().map(|c| ColumnWithName::new(c.name().clone(), c.data().clone())).collect(),
			SystemColumns::new(
				vec![stable_rn],
				Vec::new(),
				cols.created_at().to_vec(),
				cols.updated_at().to_vec(),
				cols.time().to_vec(),
			),
		)
	}

	fn published(columns: &Columns, rows: &[(usize, RowNumber)]) -> Columns {
		let indices: Vec<usize> = rows.iter().map(|&(row_idx, _)| row_idx).collect();
		let source = columns.extract_by_indices(&indices);
		Columns::with_system(
			source.iter().map(|c| ColumnWithName::new(c.name().clone(), c.data().clone())).collect(),
			SystemColumns::new(
				rows.iter().map(|&(_, stable_rn)| stable_rn).collect(),
				Vec::new(),
				source.created_at().to_vec(),
				source.updated_at().to_vec(),
				source.time().to_vec(),
			),
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
				params: &self.ctx.params,
				symbols: &self.ctx.symbols,
				routines: &self.routines,
				runtime_context: &self.runtime_context,
				identity: self.ctx.identity,
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
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &HashMap<Hash128, GroupId>,
		columns: &Columns,
	) -> Result<Vec<Diff>> {
		let mut result = Vec::new();
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(result);
		}

		if state.layout.update_from_columns(columns) {
			state.layout_changed_at = Some(row_time(host, columns, 0));
		}
		let hashes = self.compute_hashes(columns)?;

		let mut order: Vec<usize> = (0..row_count).collect();
		if !columns.row_numbers().is_empty() {
			order.sort_by(|&a, &b| columns.row_numbers()[b].cmp(&columns.row_numbers()[a]));
		}

		let mut new_entries: Vec<(usize, Hash128)> = Vec::new();
		let mut swap_pairs: Vec<(SerializedRow, usize, Hash128)> = Vec::new();

		for &row_idx in &order {
			let hash = hashes[row_idx];
			let row_number = columns.row_numbers()[row_idx];
			let new_serialized = SerializedRow::from_columns_at_index(columns, row_idx);

			if let Some(entry) = state.entries.get_mut(&hash) {
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
					},
				);
				new_entries.push((row_idx, hash));
			}
		}
		for (row_idx, &hash) in hashes.iter().enumerate() {
			state.dirty.insert(hash, row_time(host, columns, row_idx));
		}

		new_entries.sort_by_key(|&(i, _)| columns.row_numbers()[i]);
		swap_pairs.sort_by_key(|&(_, i, _)| columns.row_numbers()[i]);

		let mut minted: Vec<(usize, RowNumber)> = Vec::with_capacity(new_entries.len());
		let mut republished: Vec<(usize, RowNumber)> = Vec::new();
		if !new_entries.is_empty() {
			let group_ids: Vec<GroupId> = new_entries.iter().map(|&(_, hash)| groups[&hash]).collect();
			let stable_rns = host.get_or_create_row_numbers_for_groups(&group_ids)?;
			for (&(row_idx, _), (stable_rn, is_new)) in new_entries.iter().zip(stable_rns) {
				if is_new {
					minted.push((row_idx, stable_rn));
				} else {
					republished.push((row_idx, stable_rn));
				}
			}
		}
		if !minted.is_empty() {
			result.push(Diff::insert(Self::published(columns, &minted)));
		}
		if !republished.is_empty() {
			let output = Self::published(columns, &republished);
			result.push(Diff::update(output.clone(), output));
		}

		if !swap_pairs.is_empty() {
			let group_ids: Vec<GroupId> = swap_pairs.iter().map(|&(_, _, hash)| groups[&hash]).collect();
			let stable_rns = host.get_or_create_row_numbers_for_groups(&group_ids)?;
			for ((old_serialized, new_idx, _), (stable_rn, _)) in swap_pairs.into_iter().zip(stable_rns) {
				let pre_cols =
					Self::with_stable_rn(old_serialized.to_columns(&state.layout), stable_rn);
				let post_cols = Self::with_stable_rn(columns.extract_by_indices(&[new_idx]), stable_rn);
				result.push(Diff::update(pre_cols, post_cols));
			}
		}

		Ok(result)
	}

	pub(super) fn process_update(
		&self,
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &HashMap<Hash128, GroupId>,
		pre_columns: &Columns,
		post_columns: &Columns,
	) -> Result<Vec<Diff>> {
		let row_count = post_columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if state.layout.update_from_columns(post_columns) {
			state.layout_changed_at = Some(row_time(host, post_columns, 0));
		}
		let pre_hashes = self.compute_hashes(pre_columns)?;
		let post_hashes = self.compute_hashes(post_columns)?;

		let mut result = Vec::new();
		let mut dropped = 0u64;

		for row_idx in 0..row_count {
			let pre_hash = pre_hashes[row_idx];
			let post_hash = post_hashes[row_idx];
			let row_number = post_columns.row_numbers()[row_idx];

			if pre_hash == post_hash {
				let new_serialized = SerializedRow::from_columns_at_index(post_columns, row_idx);
				let visible = if let Some(entry) = state.entries.get_mut(&pre_hash) {
					let visible_rn = entry.rows.keys().next_back().copied();
					entry.rows.insert(row_number, new_serialized);
					state.dirty.insert(pre_hash, row_time(host, post_columns, row_idx));
					visible_rn == Some(row_number)
				} else {
					dropped += 1;
					false
				};
				if visible {
					let (stable_rn, _) = host
						.get_or_create_row_numbers_for_groups(&[groups[&pre_hash]])?
						.into_iter()
						.next()
						.unwrap();
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
						state.dirty.insert(pre_hash, row_time(host, post_columns, row_idx));
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
					dropped += 1;
					None
				}
			};

			if state.entries.get(&pre_hash).map(|e| e.rows.is_empty()).unwrap_or(false) {
				state.entries.shift_remove(&pre_hash);
			}

			let new_serialized = SerializedRow::from_columns_at_index(post_columns, row_idx);
			let post_mutation: (bool, Option<SerializedRow>) =
				if let Some(entry) = state.entries.get_mut(&post_hash) {
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
						},
					);
					(true, None)
				};
			state.dirty.insert(post_hash, row_time(host, post_columns, row_idx));

			if let Some((pre_is_empty, pre_new_visible_opt)) = pre_mutation {
				let (stable_rn, _) = host
					.get_or_create_row_numbers_for_groups(&[groups[&pre_hash]])?
					.into_iter()
					.next()
					.unwrap();
				if pre_is_empty {
					host.remove_row_number_for_group(groups[&pre_hash])?;
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
				let (stable_rn, minted) = host
					.get_or_create_row_numbers_for_groups(&[groups[&post_hash]])?
					.into_iter()
					.next()
					.unwrap();
				let post_out =
					Self::with_stable_rn(post_columns.extract_by_indices(&[row_idx]), stable_rn);
				match post_displaced_opt {
					Some(old_visible) => result.push(Diff::update(
						Self::with_stable_rn(old_visible.to_columns(&state.layout), stable_rn),
						post_out,
					)),
					None if minted => result.push(Diff::insert(post_out)),
					None => result.push(Diff::update(post_out.clone(), post_out)),
				}
			}
		}

		self.dropped.note(dropped);
		Ok(result)
	}

	pub(super) fn process_remove(
		&self,
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &HashMap<Hash128, GroupId>,
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
		let mut dropped = 0u64;

		for (row_idx, &hash) in hashes.iter().enumerate() {
			let row_number = columns.row_numbers()[row_idx];

			let Some(entry) = state.entries.get_mut(&hash) else {
				dropped += 1;
				continue;
			};

			let prev_rn = entry.rows.keys().next_back().copied().unwrap();
			let removed = entry.rows.remove(&row_number).is_some();
			if !removed {
				continue;
			}
			state.dirty.insert(hash, row_time(host, columns, row_idx));

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

		let active: Vec<(usize, Hash128, Option<SerializedRow>)> = mutations
			.into_iter()
			.filter_map(|(row_idx, hash, mutation)| {
				mutation.map(|new_visible_opt| (row_idx, hash, new_visible_opt))
			})
			.collect();
		if !active.is_empty() {
			let group_ids: Vec<GroupId> = active.iter().map(|&(_, hash, _)| groups[&hash]).collect();
			let stable_rns = host.get_or_create_row_numbers_for_groups(&group_ids)?;
			for ((row_idx, hash, new_visible_opt), (stable_rn, _)) in active.into_iter().zip(stable_rns) {
				match new_visible_opt {
					None => {
						host.remove_row_number_for_group(groups[&hash])?;
						result.push(Diff::remove(Self::with_stable_rn(
							columns.extract_by_indices(&[row_idx]),
							stable_rn,
						)));
					}
					Some(new_visible) => {
						result.push(Diff::update(
							Self::with_stable_rn(
								columns.extract_by_indices(&[row_idx]),
								stable_rn,
							),
							Self::with_stable_rn(
								new_visible.to_columns(&state.layout),
								stable_rn,
							),
						));
					}
				}
			}
		}

		self.dropped.note(dropped);
		Ok(result)
	}
}
