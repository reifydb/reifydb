// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The aggregation corpus: a group column and a value column, and nothing else. An aggregate routes
//! every row into one degenerate span, so unlike the window family there is no time dimension for the
//! corpus to vary - what varies is how often two rows land in the same group and how often two values
//! tie.

use rand::{RngExt, rngs::StdRng};
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change};
use reifydb_testing_chaos::operator::workload::{Lanes, Workload};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::framework::generator;

pub const SOURCE_OPERATOR: OperatorId = OperatorId(0);
pub const AGGREGATE_OPERATOR: OperatorId = OperatorId(1);

/// Rows are stamped from their own number rather than a constant, so an output row carrying a time is
/// carrying one the corpus can be traced back to. Past the epoch so a zeroed timestamp is visibly
/// different from a stamped one.
const BASE_MS: u64 = 1_000_000;

#[derive(Clone, Debug)]
pub struct AggregateRow {
	pub number: RowNumber,
	pub group: i32,
	pub value: i64,
}

impl AggregateRow {
	fn at(&self) -> DateTime {
		DateTime::from_epoch_millis(BASE_MS + self.number.0).expect("a row stamp is representable")
	}
}

pub struct AggregateWorkload {
	pub groups: i32,

	/// The inclusive ceiling on a drawn value. Deliberately reaches low: `math::min` and `math::max`
	/// can only invert a retraction when the retracted value is not the one the group currently
	/// reports, so ties are what force the accumulator down its full-recompute path.
	pub value_ceiling: i64,
}

impl Workload for AggregateWorkload {
	type Row = AggregateRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> AggregateRow {
		// Two draws, group before value. The order and count are what a pinned corpus is recorded
		// against, so changing either re-points every regression pin at a different sequence.
		AggregateRow {
			number,
			group: rng.random_range(1..=self.groups),
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &AggregateRow) -> AggregateRow {
		// The group is held fixed so an update stays an update. Moving a row between groups is a
		// different path and belongs in its own sweep, not folded into this one silently.
		AggregateRow {
			value: rng.random_range(1..=self.value_ceiling),
			..row.clone()
		}
	}

	fn lanes(&self, row: &AggregateRow) -> Lanes {
		Lanes {
			number: row.number.0,
			group: row.group as u64,
			coord: BASE_MS + row.number.0,
			value: row.value as u64,
		}
	}

	fn insert(&self, rows: &[AggregateRow]) -> Change {
		generator::insert(rows.iter().map(|r| generator::row(r.number, r.group, r.value, r.at())).collect())
	}

	fn remove(&self, row: &AggregateRow) -> Change {
		generator::remove(vec![generator::row(row.number, row.group, row.value, row.at())])
	}

	fn update(&self, pre: &AggregateRow, post: &AggregateRow) -> Change {
		generator::update(vec![(
			generator::row(pre.number, pre.group, pre.value, pre.at()),
			generator::row(post.number, post.group, post.value, post.at()),
		)])
	}

	fn projection(&self) -> &[usize] {
		&[0, 1]
	}
}
