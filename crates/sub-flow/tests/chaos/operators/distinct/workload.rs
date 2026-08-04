// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The distinct corpus: a key column and a payload column.
//!
//! The two must be separate for the suite to prove anything. Distinct over every column makes the key
//! and the content the same thing, so which of several colliding rows the operator picks as the one it
//! publishes stops being observable. Keying on `g` alone and letting `v` vary is what makes the
//! representative visible in the output.

use rand::{RngExt, rngs::StdRng};
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change};
use reifydb_testing_chaos::operator::workload::{Lanes, Workload};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::framework::generator;

pub const SOURCE_OPERATOR: OperatorId = OperatorId(0);
pub const DISTINCT_OPERATOR: OperatorId = OperatorId(1);

/// The column the operator is told to be distinct on.
pub const KEY_COLUMN: &str = "g";

/// The column that rides along and is not part of the key, so two rows can collide on the key while
/// carrying different content.
pub const PAYLOAD_COLUMN: &str = "v";

const BASE_MS: u64 = 1_000_000;

#[derive(Clone, Debug)]
pub struct DistinctRow {
	pub number: RowNumber,
	pub group: i32,
	pub value: i64,
}

impl DistinctRow {
	fn at(&self) -> DateTime {
		DateTime::from_timestamp_millis(BASE_MS + self.number.0).expect("a row stamp is representable")
	}
}

pub struct DistinctWorkload {
	/// How many distinct keys the corpus draws from. Narrow on purpose: collisions on the key are the
	/// only thing that makes a distinct operator do any work at all.
	pub groups: i32,

	pub value_ceiling: i64,

	/// How often an update also moves the row to a different key. That is the branch in
	/// `process_update` where the row leaves one entry and joins another, which has to retract from
	/// the old key and publish into the new one in the same step - by far the widest path in the
	/// operator, and unreachable if an update only ever rewrites the payload.
	pub regroup_pct: u32,
}

impl Workload for DistinctWorkload {
	type Row = DistinctRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> DistinctRow {
		DistinctRow {
			number,
			group: rng.random_range(1..=self.groups),
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &DistinctRow) -> DistinctRow {
		// The roll is drawn unconditionally so the draw count per update does not depend on the
		// outcome; a variable-length draw would make two runs of the same seed diverge as soon as
		// regroup_pct changed.
		let regroup = rng.random_range(0..100u32) < self.regroup_pct;
		let group = match regroup {
			true => rng.random_range(1..=self.groups),
			false => row.group,
		};
		DistinctRow {
			number: row.number,
			group,
			value: rng.random_range(1..=self.value_ceiling),
		}
	}

	fn lanes(&self, row: &DistinctRow) -> Lanes {
		Lanes {
			number: row.number.0,
			group: row.group as u64,
			coord: BASE_MS + row.number.0,
			value: row.value as u64,
		}
	}

	fn insert(&self, rows: &[DistinctRow]) -> Change {
		generator::insert(rows.iter().map(|r| generator::row(r.number, r.group, r.value, r.at())).collect())
	}

	fn remove(&self, row: &DistinctRow) -> Change {
		generator::remove(vec![generator::row(row.number, row.group, row.value, row.at())])
	}

	fn update(&self, pre: &DistinctRow, post: &DistinctRow) -> Change {
		generator::update(vec![(
			generator::row(pre.number, pre.group, pre.value, pre.at()),
			generator::row(post.number, post.group, post.value, post.at()),
		)])
	}

	fn projection(&self) -> &[usize] {
		&[0, 1]
	}
}
