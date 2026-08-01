// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What append owes its consumer: every live source row, once, under a stable identity. Append
//! renumbers rather than transforms, so the claim here is thin - the sharper check is the driver
//! folding emitted diffs on the output row number, which no comparison of values would catch.

use std::collections::BTreeMap;

use reifydb_testing_chaos::operator::{
	compare::Tolerances,
	expectation::ViewClaim,
	model::Model,
	view::{MaterializedRow, MaterializedView, OutputKey},
};
use reifydb_value::value::Value;

use crate::operators::append::workload::{AppendRow, COLUMNS};

pub struct AppendOracle {
	live: BTreeMap<(usize, u64), AppendRow>,
}

impl AppendOracle {
	pub fn new() -> Self {
		Self {
			live: BTreeMap::new(),
		}
	}

	fn claim(&self) -> ViewClaim {
		let mut view = MaterializedView::empty();
		view.columns = COLUMNS.iter().map(|(name, _)| (*name).to_string()).collect();
		for row in self.live.values() {
			view.insert(
				OutputKey::new(vec![Value::Int4(row.input as i32), Value::Int8(row.source.0 as i64)]),
				MaterializedRow::from_pairs(vec![
					("src".to_string(), Value::Int4(row.input as i32)),
					("id".to_string(), Value::Int8(row.source.0 as i64)),
					("v".to_string(), Value::Int8(row.value)),
				]),
			);
		}
		// Keyed on the source identity rather than the emitted row number: the number is append's to
		// choose, but which source rows are present and what they carry is not.
		ViewClaim::new(view, vec!["src".to_string(), "id".to_string()], Tolerances::new())
	}
}

impl Model<AppendRow> for AppendOracle {
	type Expectation = ViewClaim;

	fn admit(&mut self, row: &AppendRow) -> bool {
		self.live.insert((row.input, row.source.0), row.clone());
		true
	}

	fn retract(&mut self, row: &AppendRow) {
		self.live.remove(&(row.input, row.source.0));
	}

	fn advance_ledger(&mut self, _at_ms: u64) {}

	fn live(&self) -> ViewClaim {
		self.claim()
	}

	fn all(&self) -> ViewClaim {
		self.claim()
	}

	fn after_drain(&self) -> ViewClaim {
		self.claim()
	}
}
