// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The gate corpus.
//!
//! Like take, gate does not group, so the group column carries the source row number and lets the
//! claim name which rows were admitted. The payload is what the condition is evaluated against, and
//! `revalue` deliberately draws across the whole range so a row can cross the threshold in either
//! direction during its life - upwards is what admits it, and downwards is what a gate must ignore.

use rand::{RngExt, rngs::StdRng};
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change};
use reifydb_testing_chaos::operator::workload::{Lanes, Workload};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::framework::generator;

pub const SOURCE_OPERATOR: OperatorId = OperatorId(0);
pub const GATE_OPERATOR: OperatorId = OperatorId(1);

pub const IDENTITY_COLUMN: &str = "g";

/// The column the gate condition reads.
pub const PAYLOAD_COLUMN: &str = "v";

const BASE_MS: u64 = 1_000_000;

#[derive(Clone, Debug)]
pub struct GateRow {
	pub number: RowNumber,
	pub value: i64,
}

impl GateRow {
	fn at(&self) -> DateTime {
		DateTime::from_timestamp_millis(BASE_MS + self.number.0).expect("a row stamp is representable")
	}

	pub fn identity(&self) -> i32 {
		i32::try_from(self.number.0).expect("a corpus row number fits an int4")
	}
}

pub struct GateWorkload {
	pub value_ceiling: i64,
}

impl Workload for GateWorkload {
	type Row = GateRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> GateRow {
		GateRow {
			number,
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &GateRow) -> GateRow {
		GateRow {
			number: row.number,
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn lanes(&self, row: &GateRow) -> Lanes {
		Lanes {
			number: row.number.0,
			group: row.number.0,
			coord: BASE_MS + row.number.0,
			value: row.value as u64,
		}
	}

	fn insert(&self, rows: &[GateRow]) -> Change {
		generator::insert(
			rows.iter().map(|r| generator::row(r.number, r.identity(), r.value, r.at())).collect(),
		)
	}

	fn remove(&self, row: &GateRow) -> Change {
		generator::remove(vec![generator::row(row.number, row.identity(), row.value, row.at())])
	}

	fn update(&self, pre: &GateRow, post: &GateRow) -> Change {
		generator::update(vec![(
			generator::row(pre.number, pre.identity(), pre.value, pre.at()),
			generator::row(post.number, post.identity(), post.value, post.at()),
		)])
	}

	fn projection(&self) -> &[usize] {
		&[0, 1]
	}
}
