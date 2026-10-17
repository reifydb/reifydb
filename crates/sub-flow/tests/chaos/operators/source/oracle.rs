// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What a source owes: every row it was handed, with the interned column kept as the id the corpus
//! interned.
//!
//! The id is the one the workload interned, never one read back from the operator, so nothing here
//! shares a code path with the operator.

use std::{collections::BTreeMap, sync::Arc};

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_testing_chaos::operator::model::Model;
use reifydb_value::value::{Value, dictionary::DictionaryEntryId, row_number::RowNumber};

use crate::operators::source::workload::SourceRow;

pub struct SourceOracle {
	live: BTreeMap<RowNumber, SourceRow>,
	interned: Arc<Mutex<BTreeMap<&'static str, DictionaryEntryId>>>,
}

impl SourceOracle {
	pub fn new(interned: Arc<Mutex<BTreeMap<&'static str, DictionaryEntryId>>>) -> Self {
		Self {
			live: BTreeMap::new(),
			interned,
		}
	}

	fn claim(&self) -> Vec<Vec<Value>> {
		let interned = self.interned.lock();
		let mut rows: Vec<Vec<Value>> = self
			.live
			.values()
			.map(|row| {
				let id = interned
					.get(row.symbol)
					.expect("the workload interned every symbol it emitted");
				vec![Value::DictionaryId(*id), Value::Int8(row.value)]
			})
			.collect();
		rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		rows
	}
}

impl Model<SourceRow> for SourceOracle {
	type Expectation = Vec<Vec<Value>>;

	fn admit(&mut self, row: &SourceRow) -> bool {
		self.live.insert(row.number, row.clone());
		true
	}

	fn retract(&mut self, row: &SourceRow) {
		match self.live.remove(&row.number) {
			Some(held) => assert_eq!(
				(held.symbol, held.value),
				(row.symbol, row.value),
				"the driver retracts the row it last admitted for {:?}",
				row.number
			),
			None => panic!("the driver retracted row {:?}, which the oracle never admitted", row.number),
		}
	}

	fn update(&mut self, pre: &SourceRow, post: &SourceRow) {
		assert_eq!(pre.number, post.number, "an update must not change a row's number");
		self.retract(pre);
		self.admit(post);
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// A source holds no clock and nothing in flight.
	}

	fn live(&self) -> Vec<Vec<Value>> {
		self.claim()
	}

	fn all(&self) -> Vec<Vec<Value>> {
		self.claim()
	}

	fn after_drain(&self) -> Vec<Vec<Value>> {
		self.claim()
	}
}
