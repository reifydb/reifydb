// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::NullBuffer;
use reifydb_value::value::{Value, column_view::ColumnView, dictionary::DictionaryEntryId};

struct Nones(Option<NullBuffer>);

impl Nones {
	fn of(view: &ColumnView<'_>) -> Self {
		Self(view.logical_nulls())
	}

	fn at(&self, row: usize) -> bool {
		self.0.as_ref().is_some_and(|nulls| !(row < nulls.len() && nulls.is_valid(row)))
	}
}

fn rows(views: &[ColumnView<'_>]) -> usize {
	views.first().map_or(0, ColumnView::len)
}

pub fn gather_dictionary_values(views: &[ColumnView<'_>], values: &mut Vec<Value>) {
	let nones: Vec<Nones> = views.iter().map(Nones::of).collect();
	let mut columns: Vec<_> = views.iter().map(|view| view.values().into_iter()).collect();
	values.reserve(rows(views) * views.len());
	for row in 0..rows(views) {
		for (column, nones) in columns.iter_mut().zip(&nones) {
			let value = column.next().expect("every column of a batch holds one value per row");
			if !nones.at(row) {
				values.push(value);
			}
		}
	}
}

pub fn scatter_dictionary_ids(
	views: &[ColumnView<'_>],
	none: DictionaryEntryId,
	ids: &mut impl Iterator<Item = DictionaryEntryId>,
) -> Vec<Vec<DictionaryEntryId>> {
	let nones: Vec<Nones> = views.iter().map(Nones::of).collect();
	let mut columns: Vec<Vec<DictionaryEntryId>> = views.iter().map(|_| Vec::with_capacity(rows(views))).collect();
	for row in 0..rows(views) {
		for (column, nones) in columns.iter_mut().zip(&nones) {
			let id = if nones.at(row) {
				none
			} else {
				ids.next().expect("the dictionary returns one id per interned value")
			};
			column.push(id);
		}
	}
	columns
}
