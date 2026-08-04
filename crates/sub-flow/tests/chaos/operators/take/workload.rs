// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The take corpus.
//!
//! Take does not group, so the group column of the shared row shape is free and this workload spends
//! it on the source row number. That is what lets the claim name *which* rows were retained rather
//! than only how many: take passes source row numbers through to its output, but the comparison
//! framework keys a claim on output columns, and a row number is not one until a column carries it.

use rand::{RngExt, rngs::StdRng};
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change};
use reifydb_testing_chaos::operator::workload::{Lanes, Workload};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::framework::generator;

pub const SOURCE_OPERATOR: OperatorId = OperatorId(0);
pub const TAKE_OPERATOR: OperatorId = OperatorId(1);

/// The column carrying the source row number, so the claim can say which rows survived.
pub const IDENTITY_COLUMN: &str = "g";

const BASE_MS: u64 = 1_000_000;

#[derive(Clone, Debug)]
pub struct TakeRow {
	pub number: RowNumber,
	pub value: i64,
}

impl TakeRow {
	fn at(&self) -> DateTime {
		DateTime::from_timestamp_millis(BASE_MS + self.number.0).expect("a row stamp is representable")
	}

	/// The row number as it is carried in the identity column. i32 is wide enough: a sweep runs tens
	/// of steps with batches in the single digits, so the corpus never approaches the bound.
	pub fn identity(&self) -> i32 {
		i32::try_from(self.number.0).expect("a corpus row number fits an int4")
	}
}

pub struct TakeWorkload {
	pub value_ceiling: i64,
}

impl Workload for TakeWorkload {
	type Row = TakeRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> TakeRow {
		TakeRow {
			number,
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &TakeRow) -> TakeRow {
		TakeRow {
			number: row.number,
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn lanes(&self, row: &TakeRow) -> Lanes {
		Lanes {
			number: row.number.0,
			group: row.number.0,
			coord: BASE_MS + row.number.0,
			value: row.value as u64,
		}
	}

	fn insert(&self, rows: &[TakeRow]) -> Change {
		generator::insert(
			rows.iter().map(|r| generator::row(r.number, r.identity(), r.value, r.at())).collect(),
		)
	}

	fn remove(&self, row: &TakeRow) -> Change {
		generator::remove(vec![generator::row(row.number, row.identity(), row.value, row.at())])
	}

	fn update(&self, pre: &TakeRow, post: &TakeRow) -> Change {
		generator::update(vec![(
			generator::row(pre.number, pre.identity(), pre.value, pre.at()),
			generator::row(post.number, post.identity(), post.value, post.at()),
		)])
	}

	fn projection(&self) -> &[usize] {
		&[0, 1]
	}
}
