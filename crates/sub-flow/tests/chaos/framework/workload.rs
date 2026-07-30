// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The window family's corpus: what a row is, and how it becomes a change.

use rand::{RngExt, rngs::StdRng};
use reifydb_core::interface::change::Change;
use reifydb_testing_chaos::operator::workload::{Lanes, Workload};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::framework::generator;

#[derive(Clone, Debug)]
pub struct WindowRow {
	pub number: RowNumber,
	pub group: i32,
	pub coord_ms: u64,
	pub value: i64,
}

impl WindowRow {
	pub fn at(&self) -> DateTime {
		DateTime::from_timestamp_millis(self.coord_ms).unwrap()
	}
}

pub struct WindowWorkload {
	pub groups: i32,
	pub coord_span_ms: u64,
}

impl Workload for WindowWorkload {
	type Row = WindowRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> WindowRow {
		// Group, then coordinate, then value: this order and this count of draws are what the pinned
		// window regressions were recorded against. Changing either shifts every later operation in
		// the corpus.
		WindowRow {
			number,
			group: rng.random_range(1..=self.groups),
			coord_ms: rng.random_range(0..self.coord_span_ms),
			value: rng.random_range(1..100i64),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &WindowRow) -> WindowRow {
		WindowRow {
			value: rng.random_range(1..100i64),
			..row.clone()
		}
	}

	fn lanes(&self, row: &WindowRow) -> Lanes {
		Lanes {
			number: row.number.0,
			group: row.group as u64,
			coord: row.coord_ms,
			value: row.value as u64,
		}
	}

	fn insert(&self, rows: &[WindowRow]) -> Change {
		generator::insert(rows.iter().map(|r| generator::row(r.number, r.group, r.value, r.at())).collect())
	}

	fn remove(&self, row: &WindowRow) -> Change {
		generator::remove(vec![generator::row(row.number, row.group, row.value, row.at())])
	}

	fn update(&self, pre: &WindowRow, post: &WindowRow) -> Change {
		generator::update(vec![(
			generator::row(pre.number, pre.group, pre.value, pre.at()),
			generator::row(post.number, post.group, post.value, post.at()),
		)])
	}

	fn projection(&self) -> &[usize] {
		&[0, 1]
	}
}
