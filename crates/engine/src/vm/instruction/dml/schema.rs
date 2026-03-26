// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	encoded::schema::{Schema, SchemaField},
	interface::catalog::{ringbuffer::RingBuffer, series::Series, table::Table},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

use crate::Result;

/// Get or create a schema for a table, properly handling dictionary-encoded columns
///
/// This function creates a schema with proper field names and constraints, ensuring:
/// - Dictionary-encoded columns use the dictionary ID type (not the original type)
/// - The schema is registered in the schema registry for later retrieval
/// - Field names match the table column names
pub fn get_or_create_table_schema(catalog: &Catalog, table: &Table, txn: &mut Transaction<'_>) -> Result<Schema> {
	let mut fields = Vec::with_capacity(table.columns.len());

	for col in &table.columns {
		let constraint = if let Some(dict_id) = col.dictionary_id {
			// For dictionary columns, use TypeConstraint::dictionary so the schema
			// natively produces Value::DictionaryId on get_value()
			if let Some(dict) = catalog.find_dictionary(txn, dict_id)? {
				TypeConstraint::dictionary(dict_id, dict.id_type)
			} else {
				col.constraint.clone()
			}
		} else {
			col.constraint.clone()
		};

		fields.push(SchemaField::new(col.name.clone(), constraint));
	}

	catalog.schema.get_or_create(fields)
}

/// Get or create a schema for a ring buffer, properly handling dictionary-encoded columns
///
/// This function creates a schema with proper field names and constraints, ensuring:
/// - Dictionary-encoded columns use the dictionary ID type (not the original type)
/// - The schema is registered in the schema registry for later retrieval
/// - Field names match the ring buffer column names
pub fn get_or_create_ringbuffer_schema(
	catalog: &Catalog,
	ringbuffer: &RingBuffer,
	txn: &mut Transaction<'_>,
) -> Result<Schema> {
	let mut fields = Vec::with_capacity(ringbuffer.columns.len());

	for col in &ringbuffer.columns {
		let constraint = if let Some(dict_id) = col.dictionary_id {
			// For dictionary columns, use TypeConstraint::dictionary so the schema
			// natively produces Value::DictionaryId on get_value()
			if let Some(dict) = catalog.find_dictionary(txn, dict_id)? {
				TypeConstraint::dictionary(dict_id, dict.id_type)
			} else {
				col.constraint.clone()
			}
		} else {
			col.constraint.clone()
		};

		fields.push(SchemaField::new(col.name.clone(), constraint));
	}

	catalog.schema.get_or_create(fields)
}

/// Get or create a schema for a series.
///
/// The schema includes a leading key column field followed by the series data columns.
/// This ensures series rows are encoded with a fingerprint header, enabling the CDC pipeline
/// to decode them via SchemaRegistry lookup.
pub fn get_or_create_series_schema(catalog: &Catalog, series: &Series, txn: &mut Transaction<'_>) -> Result<Schema> {
	let mut fields = Vec::with_capacity(1 + series.columns.len());
	// Find the key column's type from the declared columns
	let key_column = series.key.column();
	let key_col = series.columns.iter().find(|c| c.name == key_column);
	let key_type =
		key_col.map(|c| c.constraint.clone()).unwrap_or_else(|| TypeConstraint::unconstrained(Type::Int8));
	fields.push(SchemaField::new(key_column.to_string(), key_type));
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
		fields.push(SchemaField::new(col.name.clone(), constraint));
	}
	catalog.schema.get_or_create(fields)
}
