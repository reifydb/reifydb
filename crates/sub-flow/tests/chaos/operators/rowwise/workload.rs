// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The corpus shared by the row-wise operators: filter, map and extend.
//!
//! One workload rather than three near-copies, because all three see exactly the same input and
//! differ only in what they do with a row. Sharing it also means a divergence between two of them is
//! attributable to the operator rather than to a corpus that drifted apart.
//!
//! As with take and gate, the group column carries the source row number so a claim can name which
//! rows came through; none of these three group, so the column is otherwise unused.

use rand::{RngExt, rngs::StdRng};
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change};
use reifydb_testing_chaos::operator::workload::{Lanes, Workload};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::{framework::generator, operators::rowwise::Shape};

pub const SOURCE_OPERATOR: OperatorId = OperatorId(0);
pub const ROWWISE_OPERATOR: OperatorId = OperatorId(1);

pub const IDENTITY_COLUMN: &str = "g";

pub const PAYLOAD_COLUMN: &str = "v";

const BASE_MS: u64 = 1_000_000;

#[derive(Clone, Debug)]
pub struct RowwiseRow {
	pub number: RowNumber,
	pub value: i64,
}

impl RowwiseRow {
	fn at(&self) -> DateTime {
		DateTime::from_epoch_millis(BASE_MS + self.number.0).expect("a row stamp is representable")
	}

	pub fn identity(&self) -> i32 {
		i32::try_from(self.number.0).expect("a corpus row number fits an int4")
	}
}

pub struct RowwiseWorkload {
	pub value_ceiling: i64,

	/// Which shape's output is being compared. The driver projects a published row down to these
	/// column positions before checking it, so extend's third column is invisible to the comparison
	/// unless the projection names it - and an oracle claiming three columns against a two-column
	/// projection fails on every row for a reason that has nothing to do with the operator.
	pub shape: Shape,
}

impl Workload for RowwiseWorkload {
	type Row = RowwiseRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> RowwiseRow {
		RowwiseRow {
			number,
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &RowwiseRow) -> RowwiseRow {
		RowwiseRow {
			number: row.number,
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn lanes(&self, row: &RowwiseRow) -> Lanes {
		Lanes {
			number: row.number.0,
			group: row.number.0,
			coord: BASE_MS + row.number.0,
			value: row.value as u64,
		}
	}

	fn insert(&self, rows: &[RowwiseRow]) -> Change {
		generator::insert(
			rows.iter().map(|r| generator::row(r.number, r.identity(), r.value, r.at())).collect(),
		)
	}

	fn remove(&self, row: &RowwiseRow) -> Change {
		generator::remove(vec![generator::row(row.number, row.identity(), row.value, row.at())])
	}

	fn update(&self, pre: &RowwiseRow, post: &RowwiseRow) -> Change {
		generator::update(vec![(
			generator::row(pre.number, pre.identity(), pre.value, pre.at()),
			generator::row(post.number, post.identity(), post.value, post.at()),
		)])
	}

	fn projection(&self) -> &[usize] {
		self.shape.projection()
	}
}
