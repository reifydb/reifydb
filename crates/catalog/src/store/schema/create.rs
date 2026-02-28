// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema creation/persistence.

use reifydb_core::{
	encoded::schema::Schema,
	key::schema::{SchemaFieldKey, SchemaKey},
};
use reifydb_transaction::single::write::SingleWriteTransaction;
use tracing::instrument;

use super::schema::{schema_field, schema_header};
use crate::Result;

#[instrument(
	name = "schema_store::create",
	level = "debug",
	skip(cmd, schema),
	fields(fingerprint = ?schema.fingerprint(), field_count = schema.field_count())
)]
pub(crate) fn create_schema(cmd: &mut SingleWriteTransaction, schema: &Schema) -> Result<()> {
	let fingerprint = schema.fingerprint();

	let mut header_row = schema_header::SCHEMA.allocate();
	schema_header::SCHEMA.set_u16(&mut header_row, schema_header::FIELD_COUNT, schema.field_count() as u16);
	cmd.set(&SchemaKey::encoded(fingerprint), header_row)?;

	for (idx, field) in schema.fields().iter().enumerate() {
		let ffi = field.constraint.to_ffi();

		let mut field_row = schema_field::SCHEMA.allocate();
		schema_field::SCHEMA.set_utf8(&mut field_row, schema_field::NAME, &field.name);
		schema_field::SCHEMA.set_u8(&mut field_row, schema_field::TYPE, ffi.base_type);
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
