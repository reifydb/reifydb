// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema creation/persistence.

use reifydb_core::{
	key::{SchemaFieldKey, SchemaKey},
	schema::Schema,
};
use reifydb_transaction::standard::command::StandardCommandTransaction;

use super::layout::{schema_field, schema_header};

/// Persist a schema to storage.
///
/// This writes:
/// - The schema header (field count, row size) under SchemaKey
/// - Each field under SchemaFieldKey
pub fn create_schema(cmd: &mut StandardCommandTransaction, schema: &Schema) -> crate::Result<()> {
	let fingerprint = schema.fingerprint();

	// Write schema header
	let mut header_row = schema_header::LAYOUT.allocate();
	schema_header::LAYOUT.set_u16(&mut header_row, schema_header::FIELD_COUNT, schema.field_count() as u16);
	cmd.set(&SchemaKey::encoded(fingerprint), header_row)?;

	// Write each field
	for field in schema.fields() {
		let mut field_row = schema_field::LAYOUT.allocate();
		schema_field::LAYOUT.set_utf8(&mut field_row, schema_field::NAME, &field.name);
		schema_field::LAYOUT.set_u8(&mut field_row, schema_field::FIELD_TYPE, field.field_type.to_u8());
		schema_field::LAYOUT.set_u32(&mut field_row, schema_field::OFFSET, field.offset);
		schema_field::LAYOUT.set_u32(&mut field_row, schema_field::SIZE, field.size);
		schema_field::LAYOUT.set_u8(&mut field_row, schema_field::ALIGN, field.align);

		cmd.set(&SchemaFieldKey::encoded(fingerprint, field.field_index), field_row)?;
	}

	Ok(())
}
