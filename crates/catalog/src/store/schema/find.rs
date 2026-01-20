// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema retrieval from storage.

use reifydb_core::{
	encoded::schema::{Schema, SchemaField, fingerprint::SchemaFingerprint},
	key::{
		EncodableKey,
		schema::{SchemaFieldKey, SchemaKey},
	},
};
use reifydb_transaction::{single::svl::write::SvlCommandTransaction, standard::IntoStandardTransaction};
use reifydb_core::error::diagnostic::internal::internal;
use reifydb_type::{
	error::Error,
	value::constraint::{FFITypeConstraint, TypeConstraint},
};
use tracing::{Span, instrument};

use super::schema::{schema_field, schema_header};

/// Find a schema by its fingerprint.
///
/// Returns None if the schema doesn't exist in storage.
#[instrument(
	name = "schema_store::find",
	level = "trace",
	skip(txn),
	fields(
		fingerprint = ?fingerprint,
		found = tracing::field::Empty,
		field_count = tracing::field::Empty
	)
)]
pub(crate) fn find_schema_by_fingerprint(
	txn: &mut SvlCommandTransaction,
	fingerprint: SchemaFingerprint,
) -> crate::Result<Option<Schema>> {
	// Read schema header
	let header_key = SchemaKey::encoded(fingerprint);
	let header_entry = match txn.get(&header_key)? {
		Some(entry) => entry,
		None => {
			Span::current().record("found", false);
			Span::current().record("field_count", 0);
			return Ok(None);
		}
	};

	let field_count = schema_header::SCHEMA.get_u16(&header_entry.values, schema_header::FIELD_COUNT) as usize;

	let mut fields = Vec::with_capacity(field_count);
	for i in 0..field_count {
		let field_key = SchemaFieldKey::encoded(fingerprint, i as u16);
		let field_entry = txn.get(&field_key)?.ok_or_else(|| {
			Error(internal(format!("Schema field {} missing for fingerprint {:?}", i, fingerprint)))
		})?;

		let name = schema_field::SCHEMA.get_utf8(&field_entry.values, schema_field::NAME).to_string();
		let base_type = schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::TYPE);
		let constraint_type = schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::CONSTRAINT_TYPE);
		let constraint_param1 = schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::CONSTRAINT_P1);
		let constraint_param2 = schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::CONSTRAINT_P2);
		let constraint = TypeConstraint::from_ffi(FFITypeConstraint {
			base_type,
			constraint_type,
			constraint_param1,
			constraint_param2,
		});
		let offset = schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::OFFSET);
		let size = schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::SIZE);
		let align = schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::ALIGN);

		fields.push(SchemaField {
			name,
			constraint,
			offset,
			size,
			align,
		});
	}

	Span::current().record("found", true);
	Span::current().record("field_count", field_count);
	Ok(Some(Schema::from_parts(fingerprint, fields)))
}

/// Load all schemas from storage.
///
/// Used during startup to populate the schema registry cache.
#[instrument(
	name = "schema_store::load_all",
	level = "debug",
	skip(txn),
	fields(
		schema_count = tracing::field::Empty,
		total_fields = tracing::field::Empty
	)
)]
pub fn load_all_schemas(txn: &mut impl IntoStandardTransaction) -> crate::Result<Vec<Schema>> {
	let mut std_txn = txn.into_standard_transaction();

	// First pass: collect all schema headers (fingerprint, field_count)
	let mut schema_headers: Vec<(SchemaFingerprint, usize)> = Vec::new();

	{
		let range = SchemaKey::full_scan();
		let mut stream = std_txn.range(range, 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;

			// Decode the fingerprint from the key
			let schema_key = SchemaKey::decode(&entry.key)
				.ok_or_else(|| Error(internal("Failed to decode schema key")))?;

			let field_count =
				schema_header::SCHEMA.get_u16(&entry.values, schema_header::FIELD_COUNT) as usize;

			schema_headers.push((schema_key.fingerprint, field_count));
		}
	}

	// Second pass: load fields for each schema
	let mut schemas = Vec::with_capacity(schema_headers.len());

	for (fingerprint, field_count) in schema_headers {
		let mut fields = Vec::with_capacity(field_count);

		for i in 0..field_count {
			let field_key = SchemaFieldKey::encoded(fingerprint, i as u16);
			let field_entry = std_txn.get(&field_key)?.ok_or_else(|| {
				Error(internal(format!("Schema field {} missing for fingerprint {:?}", i, fingerprint)))
			})?;

			let name = schema_field::SCHEMA.get_utf8(&field_entry.values, schema_field::NAME).to_string();
			let base_type = schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::TYPE);
			let constraint_type =
				schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::CONSTRAINT_TYPE);
			let constraint_param1 =
				schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::CONSTRAINT_P1);
			let constraint_param2 =
				schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::CONSTRAINT_P2);
			let constraint = TypeConstraint::from_ffi(FFITypeConstraint {
				base_type,
				constraint_type,
				constraint_param1,
				constraint_param2,
			});
			let offset = schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::OFFSET);
			let size = schema_field::SCHEMA.get_u32(&field_entry.values, schema_field::SIZE);
			let align = schema_field::SCHEMA.get_u8(&field_entry.values, schema_field::ALIGN);

			fields.push(SchemaField {
				name,
				constraint,
				offset,
				size,
				align,
			});
		}

		schemas.push(Schema::from_parts(fingerprint, fields));
	}

	let total_fields: usize = schemas.iter().map(|s| s.field_count()).sum();
	tracing::Span::current().record("schema_count", schemas.len());
	tracing::Span::current().record("total_fields", total_fields);

	Ok(schemas)
}
