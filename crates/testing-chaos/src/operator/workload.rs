// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, slice};

use rand::rngs::StdRng;
use reifydb_core::interface::change::{Change, Diff, Diffs};
use reifydb_value::value::row_number::RowNumber;

pub struct Lanes {
	pub number: u64,
	pub group: u64,
	pub coord: u64,
	pub value: u64,
}

pub trait Workload {
	type Row: Clone + Debug;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> Self::Row;

	fn revalue(&self, rng: &mut StdRng, row: &Self::Row) -> Self::Row;

	fn readmit(&self, _rng: &mut StdRng, row: &Self::Row) -> Self::Row {
		row.clone()
	}

	fn lanes(&self, row: &Self::Row) -> Lanes;

	fn insert(&self, rows: &[Self::Row]) -> Change;

	fn change(&self, ops: &[Op<Self::Row>]) -> Change {
		let mut built = ops.iter().map(|op| match op {
			Op::Insert(row) => self.insert(slice::from_ref(row)),
			Op::Remove(row) => self.remove(row),
			Op::Update(pre, post) => self.update(pre, post),
		});
		let mut merged = built.next().expect("a batch carries at least one operation");
		let mut diffs: Vec<Diff> = merged.diffs.iter().cloned().collect();
		for change in built {
			diffs.extend(change.diffs.iter().cloned());
		}
		merged.diffs = Diffs::from_iter(diffs);
		merged
	}

	fn remove(&self, row: &Self::Row) -> Change;

	fn update(&self, pre: &Self::Row, post: &Self::Row) -> Change;

	fn projection(&self) -> &[usize];

	fn tolerances(&self) -> &[Option<f64>] {
		&[]
	}

	fn identity(&self, _row: &Self::Row) -> Option<Vec<u8>> {
		None
	}
}

#[derive(Debug, Clone)]
pub enum Op<R> {
	Insert(R),
	Remove(R),
	Update(R, R),
}
