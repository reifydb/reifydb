// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::RecordBatch;
use reifydb_core::{
	interface::change::{Change, Diff},
	internal_err,
};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, duration::Duration, row_number::RowNumber, system_columns::row_numbers},
};

use crate::{
	operator::{
		aggregation::{accumulator::WindowSlotKey, core::SlotRow, engine::slot_coord},
		state::seal::coord::Coord,
		window::operator::{WindowOperator, required_row_numbers},
	},
	window::{
		coord::DateTimeCoord,
		driver::rows::{RetractRows, WindowColumns, WindowDiff, WindowRows},
	},
};

pub struct BuiltinRows<'a> {
	operator: &'a WindowOperator,
	change: &'a Change,
	numbered: bool,
	sessions: bool,
	pane: Option<Duration>,
	group_values: HashMap<Hash128, Vec<Value>>,
}

impl<'a> BuiltinRows<'a> {
	pub fn new(operator: &'a WindowOperator, change: &'a Change) -> Self {
		Self {
			operator,
			change,
			numbered: true,
			sessions: false,
			pane: None,
			group_values: HashMap::new(),
		}
	}

	pub fn rolling(operator: &'a WindowOperator, change: &'a Change, pane: Option<Duration>) -> Self {
		Self {
			pane,
			..Self::new(operator, change)
		}
	}

	pub fn sessions(operator: &'a WindowOperator, change: &'a Change) -> Self {
		Self {
			numbered: false,
			sessions: true,
			..Self::new(operator, change)
		}
	}

	pub fn into_group_values(self) -> HashMap<Hash128, Vec<Value>> {
		self.group_values
	}

	fn columns(
		&mut self,
		batch: &RecordBatch,
	) -> Result<WindowColumns<Hash128, DateTimeCoord, (WindowSlotKey, SlotRow)>> {
		let row_count = batch.num_rows();
		if row_count == 0 {
			return WindowColumns::new(Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
		}
		let times = self.operator.row_times(batch, row_count)?;
		let numbers = self.numbers(batch, row_count)?;
		let groups = self.operator.core.compute_groups(batch)?;
		let slot_cols = self.operator.core.evaluate_slot_inputs(batch)?;
		let views = self.operator.core.slot_views(batch, &slot_cols)?;
		let contributions = times
			.iter()
			.zip(&numbers)
			.enumerate()
			.map(|(row, (time, number))| {
				(
					slot_coord(
						match self.pane {
							Some(pane) => DateTimeCoord(*time).floor_to(pane),
							None => DateTimeCoord(*time),
						},
						number.map_or(0, |number| number.0),
					),
					views.contribution(row, *time),
				)
			})
			.collect();
		for (hash, values) in groups.values {
			self.group_values.entry(hash).or_insert(values);
		}
		WindowColumns::new(
			(0..row_count).collect(),
			numbers,
			groups.hashes,
			times.into_iter().map(|time| Some(DateTimeCoord(time))).collect(),
			contributions,
		)
	}

	fn numbers(&self, batch: &RecordBatch, row_count: usize) -> Result<Vec<Option<RowNumber>>> {
		match self.numbered {
			true => Ok(required_row_numbers(batch)?.iter().copied().map(Some).collect()),
			false => {
				let numbers = row_numbers(batch)?;
				Ok((0..row_count).map(|row| numbers.get(row).copied()).collect())
			}
		}
	}

	fn retracts(&mut self, batch: &RecordBatch) -> Result<RetractRows<Hash128>> {
		let row_count = batch.num_rows();
		if row_count == 0 {
			return RetractRows::new(Vec::new(), Vec::new());
		}
		let numbers = self.numbers(batch, row_count)?;
		let groups = self.operator.core.compute_groups(batch)?;
		for (hash, values) in groups.values {
			self.group_values.entry(hash).or_insert(values);
		}
		RetractRows::new(numbers, groups.hashes)
	}
}

impl WindowRows for BuiltinRows<'_> {
	type Group = Hash128;
	type Coord = DateTimeCoord;
	type Contribution = (WindowSlotKey, SlotRow);

	fn diff_count(&self) -> usize {
		self.change.diffs.len()
	}

	fn diff(&mut self, index: usize) -> Result<WindowDiff<Hash128, DateTimeCoord, (WindowSlotKey, SlotRow)>> {
		let change = self.change;
		let Some(diff) = change.diffs.get(index) else {
			return internal_err!(
				"window rows asked for diff {index} of a change holding {}",
				change.diffs.len()
			);
		};
		match diff {
			Diff::Insert {
				post,
				..
			} => Ok(WindowDiff::Insert(self.columns(post)?)),
			Diff::Update {
				pre,
				post,
				..
			} => Ok(WindowDiff::Update {
				pre: self.columns(pre)?,
				post: self.columns(post)?,
			}),
			Diff::Remove {
				pre,
				..
			} => match self.sessions {
				true => Ok(WindowDiff::Retract(self.retracts(pre)?)),
				false => Ok(WindowDiff::Remove(self.columns(pre)?)),
			},
		}
	}
}
