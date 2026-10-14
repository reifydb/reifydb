// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::ArrayRef;
use arrow_buffer::NullBuffer;
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		container::dictionary_array,
		dictionary::DictionaryEntryId,
		value_type::ValueType,
	},
};

use crate::{
	internal_err,
	value::{
		batch::{batch, take_rows_or_none},
		column::builder::ColumnBuilder,
	},
};

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

pub fn decode_dictionary_ids(
	field: &FieldRef,
	array: &ArrayRef,
	target: ValueType,
	resolve: impl FnOnce(&[DictionaryEntryId]) -> Result<Vec<Option<Value>>>,
) -> Result<(FieldRef, ArrayRef)> {
	let view = ColumnView::try_from((array, field.as_ref()))?;
	let ViewData::DictionaryId {
		container,
		..
	} = view.data
	else {
		return internal_err!(
			"dictionary column {} holds {} instead of dictionary ids",
			field.name(),
			view.get_type()
		);
	};
	let mut distinct: Vec<DictionaryEntryId> = Vec::new();
	let mut positions: HashMap<u128, usize> = HashMap::new();
	let mut rows: Vec<Option<usize>> = Vec::with_capacity(view.len());
	for (row, id) in dictionary_array::iter(container).enumerate() {
		if !view.is_defined(row) {
			rows.push(None);
			continue;
		}
		let position = *positions.entry(id.to_u128()).or_insert_with(|| {
			distinct.push(id);
			distinct.len() - 1
		});
		rows.push(Some(position));
	}
	let values = match distinct.is_empty() {
		true => Vec::new(),
		false => resolve(&distinct)?,
	};
	if values.len() != distinct.len() {
		return internal_err!(
			"a dictionary batch get answered {} values for {} ids",
			values.len(),
			distinct.len()
		);
	}
	let mut found = ColumnBuilder::with_capacity(target, values.len());
	let mut slots: Vec<Option<usize>> = Vec::with_capacity(values.len());
	for value in values {
		slots.push(value.map(|value| {
			found.push_value(value);
			found.len() - 1
		}));
	}
	let picks: Vec<Option<usize>> = rows.into_iter().map(|row| row.and_then(|position| slots[position])).collect();
	let decoded = take_rows_or_none(&batch(vec![found.finish(field.name())])?, &picks)?;
	Ok((decoded.schema_ref().fields()[0].clone(), decoded.column(0).clone()))
}
