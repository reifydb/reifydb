// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The append family's corpus: N inputs numbering their own rows, and a change carrying all of them.

use rand::{RngExt, rngs::StdRng};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_testing_chaos::operator::workload::{Lanes, Op, Workload};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, datetime::DateTime, row_number::RowNumber, value_type::ValueType},
};

pub const APPEND_OPERATOR: OperatorId = OperatorId(20);

pub fn input(index: usize) -> OperatorId {
	OperatorId(21 + index as u64)
}

pub const COLUMNS: [(&str, ValueType); 3] = [("src", ValueType::Int4), ("id", ValueType::Int8), ("v", ValueType::Int8)];

#[derive(Clone, Debug)]
pub struct AppendRow {
	pub input: usize,

	/// The row number the input assigns. Inputs number independently, so this is only unique
	/// together with `input`.
	pub source: RowNumber,

	pub value: i64,
}

fn columns_of(rows: &[&AppendRow]) -> Columns {
	let mut buffers: Vec<ColumnBuffer> =
		COLUMNS.iter().map(|(_, ty)| ColumnBuffer::with_capacity(ty.clone(), rows.len())).collect();
	for row in rows {
		buffers[0].push_value(Value::Int4(row.input as i32));
		buffers[1].push_value(Value::Int8(row.source.0 as i64));
		buffers[2].push_value(Value::Int8(row.value));
	}
	let columns = COLUMNS
		.iter()
		.zip(buffers)
		.map(|((name, _), buffer)| ColumnWithName::new(Fragment::internal(*name), buffer))
		.collect();
	Columns::new(columns).with_row_numbers(rows.iter().map(|row| row.source).collect())
}

fn tagged(mut diff: Diff, idx: usize) -> Diff {
	// An untagged diff falls back to the change origin, which is input 0 here, so leaving one untagged
	// lands its rows on the first input silently rather than failing.
	diff.set_origin(Some(ChangeOrigin::Flow(input(idx))));
	diff
}

fn change(diffs: Vec<Diff>) -> Change {
	Change::from_flow(input(0), CommitVersion(1), diffs, DateTime::default())
}

pub struct AppendWorkload {
	pub inputs: usize,

	/// How many distinct row numbers an input draws from. Small on purpose: two inputs both holding a
	/// row 7 is the collision the input index in the group key exists to separate, and a corpus drawing
	/// from the driver's global counter would give every input a disjoint set and never reach it.
	pub row_space: u64,
}

impl Workload for AppendWorkload {
	type Row = AppendRow;

	fn sample(&self, rng: &mut StdRng, _number: RowNumber) -> AppendRow {
		AppendRow {
			input: rng.random_range(0..self.inputs as u32) as usize,
			source: RowNumber(1 + rng.random_range(0..self.row_space)),
			value: rng.random_range(1..100i64),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &AppendRow) -> AppendRow {
		AppendRow {
			value: rng.random_range(1..100i64),
			..row.clone()
		}
	}

	fn lanes(&self, row: &AppendRow) -> Lanes {
		Lanes {
			number: row.source.0,
			group: row.input as u64,
			coord: 0,
			value: row.value as u64,
		}
	}

	fn identity(&self, row: &AppendRow) -> Option<Vec<u8>> {
		// The same number on a different input is an unrelated row, so the input has to be part of
		// the identity or half the corpus would collapse into updates.
		let mut id = vec![row.input as u8];
		id.extend_from_slice(&row.source.0.to_le_bytes());
		Some(id)
	}

	fn insert(&self, rows: &[AppendRow]) -> Change {
		change(coalesce(&rows.iter().cloned().map(Op::Insert).collect::<Vec<_>>()))
	}

	fn remove(&self, row: &AppendRow) -> Change {
		change(vec![tagged(Diff::remove(columns_of(&[row])), row.input)])
	}

	fn update(&self, pre: &AppendRow, post: &AppendRow) -> Change {
		change(vec![tagged(Diff::update(columns_of(&[pre]), columns_of(&[post])), pre.input)])
	}

	fn change(&self, ops: &[Op<AppendRow>]) -> Change {
		change(coalesce(ops))
	}

	fn projection(&self) -> &[usize] {
		&[]
	}
}

/// Packs a run of operations into diffs the way an upstream flow would. A diff resolves against one
/// input, so a run can never span two; coalescing only consecutive operations keeps the inputs in the
/// order the driver drew them, which is what the model replays.
fn coalesce(ops: &[Op<AppendRow>]) -> Vec<Diff> {
	let mut diffs = Vec::new();
	let mut start = 0;
	while start < ops.len() {
		let mut end = start + 1;
		while end < ops.len() && continues_run(&ops[start..end], &ops[end]) {
			end += 1;
		}
		diffs.push(diff_of(&ops[start..end]));
		start = end;
	}
	diffs
}

fn continues_run(run: &[Op<AppendRow>], next: &Op<AppendRow>) -> bool {
	let same_shape = input_of(&run[0]) == input_of(next)
		&& matches!(
			(&run[0], next),
			(Op::Insert(_), Op::Insert(_))
				| (Op::Remove(_), Op::Remove(_))
				| (Op::Update(..), Op::Update(..))
		);
	// One diff never names the same row twice: append renumbers by row, so both copies would resolve
	// to one output row and the second would withdraw what the first published.
	same_shape && !run.iter().any(|op| identity_of(op) == identity_of(next))
}

fn input_of(op: &Op<AppendRow>) -> usize {
	match op {
		Op::Insert(row) | Op::Remove(row) | Op::Update(row, _) => row.input,
	}
}

fn identity_of(op: &Op<AppendRow>) -> (usize, u64) {
	match op {
		Op::Insert(row) | Op::Remove(row) | Op::Update(row, _) => (row.input, row.source.0),
	}
}

fn diff_of(run: &[Op<AppendRow>]) -> Diff {
	let input = input_of(&run[0]);
	match &run[0] {
		Op::Insert(_) => {
			let rows: Vec<&AppendRow> = run
				.iter()
				.map(|op| match op {
					Op::Insert(row) => row,
					_ => unreachable!("a run holds one kind"),
				})
				.collect();
			tagged(Diff::insert(columns_of(&rows)), input)
		}
		Op::Remove(_) => {
			let rows: Vec<&AppendRow> = run
				.iter()
				.map(|op| match op {
					Op::Remove(row) => row,
					_ => unreachable!("a run holds one kind"),
				})
				.collect();
			tagged(Diff::remove(columns_of(&rows)), input)
		}
		Op::Update(..) => {
			let pairs: Vec<(&AppendRow, &AppendRow)> = run
				.iter()
				.map(|op| match op {
					Op::Update(pre, post) => (pre, post),
					_ => unreachable!("a run holds one kind"),
				})
				.collect();
			let pre: Vec<&AppendRow> = pairs.iter().map(|(pre, _)| *pre).collect();
			let post: Vec<&AppendRow> = pairs.iter().map(|(_, post)| *post).collect();
			tagged(Diff::update(columns_of(&pre), columns_of(&post)), input)
		}
	}
}
