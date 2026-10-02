// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	interface::catalog::dictionary::Dictionary,
	internal_err,
	value::{
		batch::{batch, take_rows_or_none},
		column::builder::ColumnBuilder,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::{
		column_view::{ColumnView, ViewData},
		container::dictionary_array,
		system_columns::{is_system_field, user_columns},
		value_type::ValueType,
	},
};
use tracing::instrument;

use crate::{Result, transaction::operation::dictionary::DictionaryOperations};

#[instrument(level = "trace", skip_all, name = "volcano::scan::dictionaries")]
pub(crate) fn decode_dictionary_columns(
	input: RecordBatch,
	dictionaries: &[Option<(Dictionary, ValueType)>],
	rx: &mut Transaction,
) -> Result<RecordBatch> {
	if dictionaries.iter().all(Option::is_none) {
		return Ok(input);
	}
	let schema = input.schema();
	let mut columns = Vec::with_capacity(input.num_columns());
	let mut user_index = 0usize;
	for (field, array) in schema.fields().iter().zip(input.columns()) {
		if is_system_field(field) {
			columns.push((field.clone(), array.clone()));
			continue;
		}
		let dictionary = dictionaries.get(user_index).and_then(Option::as_ref);
		user_index += 1;
		let Some((dictionary, declared)) = dictionary else {
			columns.push((field.clone(), array.clone()));
			continue;
		};
		columns.push(decode_dictionary_column(field, array, dictionary, declared.clone(), rx)?);
	}
	batch(columns)
}

pub(crate) fn decode_dictionary_column(
	field: &FieldRef,
	array: &ArrayRef,
	dictionary: &Dictionary,
	target: ValueType,
	rx: &mut Transaction,
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
	let mut slots: HashMap<u128, Option<usize>> = HashMap::new();
	let mut values = ColumnBuilder::with_capacity(target, 0);
	let mut picks = Vec::with_capacity(view.len());
	for (row, id) in dictionary_array::iter(container).enumerate() {
		if !view.is_defined(row) {
			picks.push(None);
			continue;
		}
		let key = id.to_u128();
		if let Some(&slot) = slots.get(&key) {
			picks.push(slot);
			continue;
		}
		let slot = match rx.get_from_dictionary(dictionary, id)? {
			Some(value) => {
				values.push_value(value);
				Some(values.len() - 1)
			}
			None => None,
		};
		slots.insert(key, slot);
		picks.push(slot);
	}
	let decoded = take_rows_or_none(&batch(vec![values.finish(field.name())])?, &picks)?;
	Ok((decoded.schema_ref().fields()[0].clone(), decoded.column(0).clone()))
}

pub(crate) fn user_pairs(batch: &RecordBatch) -> Vec<(FieldRef, ArrayRef)> {
	user_columns(batch).map(|(field, array)| (field.clone(), array.clone())).collect()
}

pub(crate) fn user_header_names(headers: &ColumnHeaders) -> Vec<Fragment> {
	headers.columns.iter().filter(|name| !name.text().starts_with('#')).cloned().collect()
}

pub(crate) fn with_system_headers(user: Vec<Fragment>, from: &ColumnHeaders) -> ColumnHeaders {
	let mut columns = user;
	columns.extend(from.columns.iter().filter(|name| name.text().starts_with('#')).cloned());
	ColumnHeaders {
		columns,
	}
}

pub(crate) fn with_user_columns(user: Vec<(FieldRef, ArrayRef)>, from: &RecordBatch) -> Result<RecordBatch> {
	let mut columns = user;
	for (field, array) in from.schema_ref().fields().iter().zip(from.columns()) {
		if is_system_field(field) {
			columns.push((field.clone(), array.clone()));
		}
	}
	batch(columns)
}

use query::{QueryContext, QueryNode};
use reifydb_core::value::column::headers::ColumnHeaders;

pub(crate) struct NoopNode;

impl QueryNode for NoopNode {
	fn initialize<'a>(&mut self, _: &mut Transaction<'a>, _: &QueryContext) -> Result<()> {
		Ok(())
	}
	fn next<'a>(&mut self, _: &mut Transaction<'a>, _: &mut QueryContext) -> Result<Option<RecordBatch>> {
		Ok(None)
	}
	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}

pub mod aggregate;
pub mod append;
pub mod apply_transform;
pub mod assert;
pub mod compile;
pub mod distinct;
pub mod environment;
pub mod extend;
pub mod filter;
pub mod generator;
pub mod inline;
pub mod join;
pub(crate) mod key_rows;
pub mod lookup;
pub mod map;
pub mod patch;
pub mod query;
pub(crate) mod rank;
pub mod row_lookup;
pub mod run_tests;
pub mod scalarize;
pub mod scan;
pub mod sort;
pub mod take;
pub mod top_k;
pub(crate) mod udf;
pub mod variable;
pub mod window;
