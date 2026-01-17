// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema creation/persistence.

use reifydb_core::{
	key::{SchemaFieldKey, SchemaKey},
	schema::Schema,
};
use reifydb_transaction::standard::command::StandardCommandTransaction;

use super::schema::{schema_field, schema_header};

/// Persist a schema to storage.
///
/// This writes:
/// - The schema header (field count, row size) under SchemaKey
/// - Each field under SchemaFieldKey
pub fn create_schema(cmd: &mut StandardCommandTransaction, schema: &Schema) -> crate::Result<()> {
	let fingerprint = schema.fingerprint();

	// Write schema header
	let mut header_row = schema_header::SCHEMA.allocate();
	schema_header::SCHEMA.set_u16(&mut header_row, schema_header::FIELD_COUNT, schema.field_count() as u16);
	cmd.set(&SchemaKey::encoded(fingerprint), header_row)?;

	// Write each field
	for (idx, field) in schema.fields().iter().enumerate() {
		let ffi = field.constraint.to_ffi();

		let mut field_row = schema_field::SCHEMA.allocate();
		schema_field::SCHEMA.set_utf8(&mut field_row, schema_field::NAME, &field.name);
		schema_field::SCHEMA.set_u8(&mut field_row, schema_field::BASE_TYPE, ffi.base_type);
		schema_field::SCHEMA.set_u8(&mut field_row, schema_field::CONSTRAINT_TYPE, ffi.constraint_type);
		schema_field::SCHEMA.set_u32(&mut field_row, schema_field::CONSTRAINT_P1, ffi.constraint_param1);
		schema_field::SCHEMA.set_u32(&mut field_row, schema_field::CONSTRAINT_P2, ffi.constraint_param2);
		schema_field::SCHEMA.set_u32(&mut field_row, schema_field::OFFSET, field.offset);
		schema_field::SCHEMA.set_u32(&mut field_row, schema_field::SIZE, field.size);
		schema_field::SCHEMA.set_u8(&mut field_row, schema_field::ALIGN, field.align);

		cmd.set(&SchemaFieldKey::encoded(fingerprint, idx as u16), field_row)?;
	}

	Ok(())
}
