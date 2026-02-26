// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	encoded::schema::{Schema, SchemaField},
	interface::catalog::{ringbuffer::RingBufferDef, series::SeriesDef, table::TableDef},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

/// Get or create a schema for a table, properly handling dictionary-encoded columns
///
/// This function creates a schema with proper field names and constraints, ensuring:
/// - Dictionary-encoded columns use the dictionary ID type (not the original type)
/// - The schema is registered in the schema registry for later retrieval
/// - Field names match the table column names
pub fn get_or_create_table_schema(
	catalog: &Catalog,
	table: &TableDef,
	txn: &mut Transaction<'_>,
) -> crate::Result<Schema> {
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
	ringbuffer: &RingBufferDef,
	txn: &mut Transaction<'_>,
) -> crate::Result<Schema> {
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
/// The schema includes a leading `timestamp` (Int8) field followed by the series data columns.
/// This ensures series rows are encoded with a fingerprint header, enabling the CDC pipeline
/// to decode them via SchemaRegistry lookup.
pub fn get_or_create_series_schema(
	catalog: &Catalog,
	series: &SeriesDef,
	txn: &mut Transaction<'_>,
) -> crate::Result<Schema> {
	let mut fields = Vec::with_capacity(1 + series.columns.len());
	fields.push(SchemaField::new("timestamp".to_string(), TypeConstraint::unconstrained(Type::Int8)));
	for col in &series.columns {
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
