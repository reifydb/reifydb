// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A corpus of dictionary-encoded rows.
//!
//! Driven encoded rather than plain because a source operator over plain columns is the identity
//! function, and a sweep against it would restate the workload. The interned column is the only part
//! of a source's contract that can be wrong.

use std::{
	collections::BTreeMap,
	sync::{Arc, Mutex},
};

use rand::{RngExt, rngs::StdRng};
use reifydb_core::{
	interface::{
		catalog::dictionary::Dictionary,
		change::{Change, Diff},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_testing_chaos::operator::workload::{Lanes, Workload};
use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value, datetime::DateTime, dictionary::DictionaryEntryId, row_number::RowNumber,
		system_columns::SystemColumns, value_type::ValueType,
	},
};

pub const SYMBOL_COLUMN: &str = "sym";
pub const VALUE_COLUMN: &str = "v";

const BASE_MS: u64 = 1_000_000;

/// The pool the corpus interns from. Small on purpose: repeats are what make a decode serve the same
/// id twice, which is where an aliasing cache would show up.
pub const SYMBOLS: [&str; 4] = ["sol", "eth", "btc", "usdc"];

#[derive(Clone, Debug)]
pub struct SourceRow {
	pub number: RowNumber,
	pub symbol: &'static str,
	pub value: i64,
}

impl SourceRow {
	fn at(&self) -> DateTime {
		DateTime::from_epoch_millis(BASE_MS + self.number.0).expect("a row stamp is representable")
	}
}

/// Interns on demand and remembers what it handed out, so the oracle can state the decoded value
/// without asking the operator's own decode path.
pub struct SourceWorkload {
	pub dictionary: Dictionary,
	pub registry: DictionaryAllocatorRegistry,
	pub interned: Arc<Mutex<BTreeMap<&'static str, DictionaryEntryId>>>,
}

impl SourceWorkload {
	pub fn entry_id(&self, symbol: &'static str) -> DictionaryEntryId {
		let mut held = self.interned.lock().expect("the intern map is not poisoned");
		held.entry(symbol)
			.or_insert_with(|| {
				self.registry
					.intern(&self.dictionary, &Value::Utf8(symbol.to_string()))
					.expect("interning a symbol succeeds")
					.id
			})
			.clone()
	}

	fn columns(&self, rows: &[SourceRow]) -> Columns {
		let mut symbols = ColumnBuffer::with_capacity(ValueType::DictionaryId, rows.len());
		let mut values = ColumnBuffer::with_capacity(ValueType::Int8, rows.len());
		for row in rows {
			symbols.push_value(self.entry_id(row.symbol).to_value());
			values.push_value(Value::Int8(row.value));
		}
		if let ColumnBuffer::DictionaryId(container) = &mut symbols {
			container.set_dictionary_id(self.dictionary.id);
		}

		let stamps: Vec<DateTime> = rows.iter().map(|row| row.at()).collect();
		Columns::with_system(
			vec![
				ColumnWithName::new(Fragment::internal(SYMBOL_COLUMN), symbols),
				ColumnWithName::new(Fragment::internal(VALUE_COLUMN), values),
			],
			SystemColumns::new(
				rows.iter().map(|row| row.number).collect(),
				Vec::new(),
				stamps.clone(),
				stamps.clone(),
				stamps,
			),
		)
	}

	fn change(&self, diff: Diff) -> Change {
		Change::from_flow(
			crate::operators::source::SOURCE,
			reifydb_core::common::CommitVersion(1),
			vec![diff],
			DateTime::default(),
		)
	}
}

impl Workload for SourceWorkload {
	type Row = SourceRow;

	fn sample(&self, rng: &mut StdRng, number: RowNumber) -> SourceRow {
		SourceRow {
			number,
			symbol: SYMBOLS[rng.random_range(0..SYMBOLS.len())],
			value: rng.random_range(1..=100i64),
		}
	}

	fn revalue(&self, rng: &mut StdRng, row: &SourceRow) -> SourceRow {
		// The symbol moves too, so the two halves of an update carry different ids.
		SourceRow {
			symbol: SYMBOLS[rng.random_range(0..SYMBOLS.len())],
			value: rng.random_range(1..=100i64),
			..row.clone()
		}
	}

	fn lanes(&self, row: &SourceRow) -> Lanes {
		Lanes {
			number: row.number.0,
			group: SYMBOLS.iter().position(|s| *s == row.symbol).expect("a drawn symbol is in the pool")
				as u64,
			coord: BASE_MS + row.number.0,
			value: row.value as u64,
		}
	}

	fn insert(&self, rows: &[SourceRow]) -> Change {
		self.change(Diff::insert(self.columns(rows)))
	}

	fn remove(&self, row: &SourceRow) -> Change {
		self.change(Diff::remove(self.columns(std::slice::from_ref(row))))
	}

	fn update(&self, pre: &SourceRow, post: &SourceRow) -> Change {
		self.change(Diff::update(
			self.columns(std::slice::from_ref(pre)),
			self.columns(std::slice::from_ref(post)),
		))
	}

	fn projection(&self) -> &[usize] {
		&[0, 1]
	}
}
