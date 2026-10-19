// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	mem::take,
	sync::Arc,
};

use arrow_array::{RecordBatch, UInt64Array};
use reifydb_codec::key::serializer::KeySerializer;
use reifydb_core::{
	error::diagnostic::operation::distinct_key_unkeyable,
	interface::change::Diff,
	key::operator::state::GroupId,
	value::{
		batch::{empty_batch, take_rows},
		column::key::extend_keys,
	},
};
use reifydb_evaluate::expression::context::EvalContext;
use reifydb_value::{
	Result, error,
	fragment::Fragment,
	util::hash::{Hash128, xxh3_128},
	value::{
		column_view::ColumnView,
		row_number::RowNumber,
		system_columns::{SystemColumn, require_row_numbers, restamp_row_numbers},
	},
};

use crate::operator::{
	distinct::{
		operator::DistinctPlan,
		state::{DistinctEntry, DistinctLayout, DistinctState, SerializedRow, user_views},
	},
	host::HostContext,
};

const CARRIED_SYSTEM_COLUMNS: [SystemColumn; 3] =
	[SystemColumn::CreatedAt, SystemColumn::UpdatedAt, SystemColumn::Time];

fn ensure_distinct_keyable(view: &ColumnView) -> Result<()> {
	let ty = view.get_type();
	if !ty.is_scalar() {
		return Err(error!(distinct_key_unkeyable(Fragment::internal(view.field.name()), ty)));
	}
	Ok(())
}

fn row_hashes(key_columns: &[ColumnView], row_count: usize) -> Result<Vec<Hash128>> {
	for view in key_columns {
		ensure_distinct_keyable(view)?;
	}
	let mut rows: Vec<KeySerializer> = (0..row_count).map(|_| KeySerializer::new()).collect();
	for view in key_columns {
		extend_keys(view, &mut rows)?;
	}
	Ok(rows.into_iter().map(|row| xxh3_128(row.to_encoded_key().as_bytes())).collect())
}

fn restamped(columns: &RecordBatch, row_numbers: Vec<u64>) -> Result<RecordBatch> {
	restamp_row_numbers(columns, &CARRIED_SYSTEM_COLUMNS, Arc::new(UInt64Array::from(row_numbers)))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
	Insert,
	Update,
	Remove,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Source {
	Absent,
	Pre,
	Post,
	Stored,
}

enum Cell {
	Pre(usize),
	Post(usize),
	Stored(SerializedRow),
}

impl Cell {
	fn source(&self) -> Source {
		match self {
			Cell::Pre(_) => Source::Pre,
			Cell::Post(_) => Source::Post,
			Cell::Stored(_) => Source::Stored,
		}
	}
}

fn source_of(cell: &Option<Cell>) -> Source {
	cell.as_ref().map_or(Source::Absent, Cell::source)
}

struct Group {
	kind: Kind,
	pre: Vec<Cell>,
	post: Vec<Cell>,
	numbers: Vec<u64>,
}

struct Output<'a> {
	pre: &'a RecordBatch,
	post: &'a RecordBatch,
	epochs: Vec<Vec<Group>>,
	seen: HashSet<RowNumber>,
}

impl<'a> Output<'a> {
	fn new(pre: &'a RecordBatch, post: &'a RecordBatch) -> Self {
		Self {
			pre,
			post,
			epochs: vec![Vec::new()],
			seen: HashSet::new(),
		}
	}

	fn insert(&mut self, stable_rn: RowNumber, post: Cell) {
		self.push(Kind::Insert, stable_rn, None, Some(post));
	}

	fn update(&mut self, stable_rn: RowNumber, pre: Cell, post: Cell) {
		self.push(Kind::Update, stable_rn, Some(pre), Some(post));
	}

	fn remove(&mut self, stable_rn: RowNumber, pre: Cell) {
		self.push(Kind::Remove, stable_rn, Some(pre), None);
	}

	fn push(&mut self, kind: Kind, stable_rn: RowNumber, pre: Option<Cell>, post: Option<Cell>) {
		if !self.seen.insert(stable_rn) {
			self.seen.clear();
			self.seen.insert(stable_rn);
			self.epochs.push(Vec::new());
		}
		let (pre_source, post_source) = (source_of(&pre), source_of(&post));
		let epoch = self.epochs.last_mut().unwrap();
		let position = epoch.iter().position(|group| {
			group.kind == kind
				&& group.pre.first().map_or(Source::Absent, Cell::source) == pre_source
				&& group.post.first().map_or(Source::Absent, Cell::source) == post_source
		});
		let group = match position {
			Some(position) => &mut epoch[position],
			None => {
				epoch.push(Group {
					kind,
					pre: Vec::new(),
					post: Vec::new(),
					numbers: Vec::new(),
				});
				epoch.last_mut().unwrap()
			}
		};
		group.pre.extend(pre);
		group.post.extend(post);
		group.numbers.push(stable_rn.0);
	}

	fn side(&self, layout: &DistinctLayout, cells: Vec<Cell>, numbers: &[u64]) -> Result<RecordBatch> {
		let mut indices = Vec::with_capacity(cells.len());
		let mut stored = Vec::new();
		let mut batch = self.post;
		for cell in cells {
			match cell {
				Cell::Pre(row) => {
					batch = self.pre;
					indices.push(row);
				}
				Cell::Post(row) => indices.push(row),
				Cell::Stored(row) => stored.push(row),
			}
		}
		let source = match stored.is_empty() {
			true => take_rows(batch, &indices)?,
			false => layout.decode(&stored)?,
		};
		restamped(&source, numbers.to_vec())
	}

	fn finish(mut self, layout: &DistinctLayout) -> Result<Vec<Diff>> {
		let mut diffs = Vec::new();
		for group in take(&mut self.epochs).into_iter().flatten() {
			let diff = match group.kind {
				Kind::Insert => Diff::insert(self.side(layout, group.post, &group.numbers)?),
				Kind::Update => Diff::update(
					self.side(layout, group.pre, &group.numbers)?,
					self.side(layout, group.post, &group.numbers)?,
				),
				Kind::Remove => Diff::remove(self.side(layout, group.pre, &group.numbers)?),
			};
			diffs.push(diff);
		}
		Ok(diffs)
	}
}

impl DistinctPlan {
	fn stable_row_numbers(
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &[GroupId],
	) -> Result<Vec<(RowNumber, bool)>> {
		if groups.iter().any(|group| state.withdrawn.contains(group)) {
			let withdrawn: Vec<GroupId> = state.withdrawn.drain(..).collect();
			host.remove_row_numbers_for_groups(&withdrawn)?;
		}
		host.get_or_create_row_numbers_for_groups(groups)
	}

	pub(super) fn compute_hashes(&self, columns: &RecordBatch) -> Result<Vec<Hash128>> {
		let row_count = columns.num_rows();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if self.lowered_expressions.is_empty() {
			row_hashes(&user_views(columns)?, row_count)
		} else {
			let session = EvalContext {
				params: &self.ctx.params,
				symbols: &self.ctx.symbols,
				routines: &self.routines,
				runtime_context: &self.runtime_context,
				identity: self.ctx.identity,
				batch: empty_batch(),
				row_count: 1,
				target: None,
				take: None,
			};
			let exec_ctx = session.with_eval(columns.clone(), row_count);
			let mut expr_columns = Vec::new();
			for lowered_expr in &self.lowered_expressions {
				let col = lowered_expr.evaluate(&exec_ctx)?;
				expr_columns.push(col);
			}
			let key_columns: Vec<ColumnView> =
				expr_columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;
			row_hashes(&key_columns, row_count)
		}
	}

	pub(super) fn process_insert(
		&self,
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &HashMap<Hash128, GroupId>,
		columns: &RecordBatch,
		hashes: &[Hash128],
	) -> Result<Vec<Diff>> {
		let row_count = columns.num_rows();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if state.layout.update_from_columns(columns)? {
			state.layout_changed = true;
		}
		let row_numbers = require_row_numbers(columns)?;
		let mut encoded: Vec<Option<SerializedRow>> =
			state.layout.encode(columns)?.into_iter().map(Some).collect();

		let mut order: Vec<usize> = (0..row_count).collect();
		order.sort_by(|&a, &b| row_numbers[b].cmp(&row_numbers[a]));

		let mut new_entries: Vec<(usize, Hash128)> = Vec::new();
		let mut swap_pairs: Vec<(SerializedRow, usize, Hash128)> = Vec::new();

		for &row_idx in &order {
			let hash = hashes[row_idx];
			let row_number = row_numbers[row_idx];
			let new_serialized = encoded[row_idx].take().unwrap();

			if let Some(entry) = state.entries.get_mut(&hash) {
				let prev_rn = entry.rows.keys().next_back().copied().unwrap();
				let displaced = if row_number > prev_rn {
					entry.rows.get(&prev_rn).cloned()
				} else {
					None
				};
				entry.rows.insert(row_number, new_serialized);
				state.dirty.insert((hash, row_number));
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
				state.dirty.insert((hash, row_number));
				new_entries.push((row_idx, hash));
			}
		}

		new_entries.sort_by_key(|&(i, _)| row_numbers[i]);
		swap_pairs.sort_by_key(|&(_, i, _)| row_numbers[i]);

		let mut output = Output::new(columns, columns);
		let mut republished: Vec<(usize, RowNumber)> = Vec::new();
		if !new_entries.is_empty() {
			let group_ids: Vec<GroupId> = new_entries.iter().map(|&(_, hash)| groups[&hash]).collect();
			let stable_rns = Self::stable_row_numbers(host, state, &group_ids)?;
			for (&(row_idx, _), (stable_rn, is_new)) in new_entries.iter().zip(stable_rns) {
				if is_new {
					output.insert(stable_rn, Cell::Post(row_idx));
				} else {
					republished.push((row_idx, stable_rn));
				}
			}
		}
		for (row_idx, stable_rn) in republished {
			output.update(stable_rn, Cell::Post(row_idx), Cell::Post(row_idx));
		}

		if !swap_pairs.is_empty() {
			let group_ids: Vec<GroupId> = swap_pairs.iter().map(|&(_, _, hash)| groups[&hash]).collect();
			let stable_rns = Self::stable_row_numbers(host, state, &group_ids)?;
			for ((old_serialized, new_idx, _), (stable_rn, _)) in swap_pairs.into_iter().zip(stable_rns) {
				output.update(stable_rn, Cell::Stored(old_serialized), Cell::Post(new_idx));
			}
		}

		output.finish(&state.layout)
	}

	pub(super) fn process_update(
		&self,
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &HashMap<Hash128, GroupId>,
		pre_columns: &RecordBatch,
		post_columns: &RecordBatch,
		(pre_hashes, post_hashes): (&[Hash128], &[Hash128]),
	) -> Result<Vec<Diff>> {
		let row_count = post_columns.num_rows();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		if state.layout.update_from_columns(post_columns)? {
			state.layout_changed = true;
		}
		let post_row_numbers = require_row_numbers(post_columns)?;
		let mut encoded: Vec<Option<SerializedRow>> =
			state.layout.encode(post_columns)?.into_iter().map(Some).collect();

		let mut output = Output::new(pre_columns, post_columns);
		let mut dropped = 0u64;

		for row_idx in 0..row_count {
			let pre_hash = pre_hashes[row_idx];
			let post_hash = post_hashes[row_idx];
			let row_number = post_row_numbers[row_idx];

			if pre_hash == post_hash {
				let new_serialized = encoded[row_idx].take().unwrap();
				let visible = if let Some(entry) = state.entries.get_mut(&pre_hash) {
					let visible_rn = entry.rows.keys().next_back().copied();
					entry.rows.insert(row_number, new_serialized);
					state.dirty.insert((pre_hash, row_number));
					visible_rn == Some(row_number)
				} else {
					dropped += 1;
					false
				};
				if visible {
					let (stable_rn, _) =
						Self::stable_row_numbers(host, state, &[groups[&pre_hash]])?
							.into_iter()
							.next()
							.unwrap();
					output.update(stable_rn, Cell::Pre(row_idx), Cell::Post(row_idx));
				}
				continue;
			}

			let pre_mutation: Option<(bool, Option<SerializedRow>)> = {
				if let Some(entry) = state.entries.get_mut(&pre_hash) {
					let prev_rn = entry.rows.keys().next_back().copied().unwrap();
					let removed = entry.rows.remove(&row_number).is_some();
					if removed {
						state.dirty.insert((pre_hash, row_number));
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
				state.entries.swap_remove(&pre_hash);
			}

			let new_serialized = encoded[row_idx].take().unwrap();
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
			state.dirty.insert((post_hash, row_number));

			if let Some((pre_is_empty, pre_new_visible_opt)) = pre_mutation {
				let (stable_rn, _) = Self::stable_row_numbers(host, state, &[groups[&pre_hash]])?
					.into_iter()
					.next()
					.unwrap();
				if pre_is_empty {
					state.withdrawn.insert(groups[&pre_hash]);
					output.remove(stable_rn, Cell::Pre(row_idx));
				} else if let Some(new_visible) = pre_new_visible_opt {
					output.update(stable_rn, Cell::Pre(row_idx), Cell::Stored(new_visible));
				}
			}

			let (post_is_new, post_displaced_opt) = post_mutation;
			if post_is_new || post_displaced_opt.is_some() {
				let (stable_rn, minted) = Self::stable_row_numbers(host, state, &[groups[&post_hash]])?
					.into_iter()
					.next()
					.unwrap();
				match post_displaced_opt {
					Some(old_visible) => {
						output.update(stable_rn, Cell::Stored(old_visible), Cell::Post(row_idx))
					}
					None if minted => output.insert(stable_rn, Cell::Post(row_idx)),
					None => output.update(stable_rn, Cell::Post(row_idx), Cell::Post(row_idx)),
				}
			}
		}

		self.dropped.note(dropped);
		output.finish(&state.layout)
	}

	pub(super) fn process_remove(
		&self,
		host: &mut dyn HostContext,
		state: &mut DistinctState,
		groups: &HashMap<Hash128, GroupId>,
		columns: &RecordBatch,
		hashes: &[Hash128],
	) -> Result<Vec<Diff>> {
		let row_count = columns.num_rows();
		if row_count == 0 {
			return Ok(Vec::new());
		}

		let row_numbers = require_row_numbers(columns)?;

		let mut mutations: Vec<(usize, Hash128, Option<Option<SerializedRow>>)> = Vec::new();
		let mut empty_hashes: Vec<Hash128> = Vec::new();
		let mut dropped = 0u64;

		for (row_idx, &hash) in hashes.iter().enumerate() {
			let row_number = row_numbers[row_idx];

			let Some(entry) = state.entries.get_mut(&hash) else {
				dropped += 1;
				continue;
			};

			let prev_rn = entry.rows.keys().next_back().copied().unwrap();
			let removed = entry.rows.remove(&row_number).is_some();
			if !removed {
				continue;
			}
			state.dirty.insert((hash, row_number));

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
			state.entries.swap_remove(&hash);
		}

		let active: Vec<(usize, Hash128, Option<SerializedRow>)> = mutations
			.into_iter()
			.filter_map(|(row_idx, hash, mutation)| {
				mutation.map(|new_visible_opt| (row_idx, hash, new_visible_opt))
			})
			.collect();
		let mut output = Output::new(columns, columns);
		if !active.is_empty() {
			let group_ids: Vec<GroupId> = active.iter().map(|&(_, hash, _)| groups[&hash]).collect();
			let stable_rns = Self::stable_row_numbers(host, state, &group_ids)?;
			for ((row_idx, hash, new_visible_opt), (stable_rn, _)) in active.into_iter().zip(stable_rns) {
				match new_visible_opt {
					None => {
						state.withdrawn.insert(groups[&hash]);
						output.remove(stable_rn, Cell::Pre(row_idx));
					}
					Some(new_visible) => {
						output.update(stable_rn, Cell::Pre(row_idx), Cell::Stored(new_visible))
					}
				}
			}
		}

		self.dropped.note(dropped);
		output.finish(&state.layout)
	}
}
