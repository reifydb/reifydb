// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	encoded::shape::{RowShape, RowShapeField},
	interface::catalog::{ringbuffer::RingBuffer, series::Series, table::Table},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use crate::Result;

pub fn get_or_create_table_shape(catalog: &Catalog, table: &Table, txn: &mut Transaction<'_>) -> Result<RowShape> {
	let mut fields = Vec::with_capacity(table.columns.len());

	for col in &table.columns {
		let constraint = if let Some(dict_id) = col.dictionary_id {
			if let Some(dict) = catalog.find_dictionary(txn, dict_id)? {
				TypeConstraint::dictionary(dict_id, dict.id_type)
			} else {
				col.constraint.clone()
			}
		} else {
			col.constraint.clone()
		};

		fields.push(RowShapeField::new(col.name.clone(), constraint));
	}

	catalog.get_or_create_row_shape(txn, fields)
}

pub fn get_or_create_ringbuffer_shape(
	catalog: &Catalog,
	ringbuffer: &RingBuffer,
	txn: &mut Transaction<'_>,
) -> Result<RowShape> {
	let mut fields = Vec::with_capacity(ringbuffer.columns.len());

	for col in &ringbuffer.columns {
		let constraint = if let Some(dict_id) = col.dictionary_id {
			if let Some(dict) = catalog.find_dictionary(txn, dict_id)? {
				TypeConstraint::dictionary(dict_id, dict.id_type)
			} else {
				col.constraint.clone()
			}
		} else {
			col.constraint.clone()
		};

		fields.push(RowShapeField::new(col.name.clone(), constraint));
	}

	catalog.get_or_create_row_shape(txn, fields)
}

pub fn get_or_create_series_shape(catalog: &Catalog, series: &Series, txn: &mut Transaction<'_>) -> Result<RowShape> {
	let mut fields = Vec::with_capacity(1 + series.columns.len());

	let key_column = series.key.column();
	let key_col = series.columns.iter().find(|c| c.name == key_column);
	let key_type =
		key_col.map(|c| c.constraint.clone()).unwrap_or_else(|| TypeConstraint::unconstrained(Type::Int8));
	fields.push(RowShapeField::new(key_column.to_string(), key_type));
	for col in series.data_columns() {
		let constraint = if let Some(dict_id) = col.dictionary_id {
			if let Some(dict) = catalog.find_dictionary(txn, dict_id)? {
				TypeConstraint::dictionary(dict_id, dict.id_type)
			} else {
				col.constraint.clone()
			}
		} else {
			col.constraint.clone()
		};
		fields.push(RowShapeField::new(col.name.clone(), constraint));
	}
	catalog.get_or_create_row_shape(txn, fields)
}
