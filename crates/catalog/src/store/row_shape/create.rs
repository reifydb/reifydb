// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowShape creation/persistence.

use reifydb_core::{
	encoded::shape::RowShape,
	key::shape::{RowShapeFieldKey, RowShapeKey},
};
use reifydb_transaction::single::write::SingleWriteTransaction;
use tracing::instrument;

use super::shape::{shape_field, shape_header};
use crate::Result;

#[instrument(
	name = "shape_store::create",
	level = "debug",
	skip(cmd, shape),
	fields(fingerprint = ?shape.fingerprint(), field_count = shape.field_count())
)]
pub(crate) fn create_row_shape(cmd: &mut SingleWriteTransaction, shape: &RowShape) -> Result<()> {
	let fingerprint = shape.fingerprint();

	let mut header_row = shape_header::SHAPE.allocate();
	shape_header::SHAPE.set_u16(&mut header_row, shape_header::FIELD_COUNT, shape.field_count() as u16);
	cmd.set(&RowShapeKey::encoded(fingerprint), header_row)?;

	for (idx, field) in shape.fields().iter().enumerate() {
		let ffi = field.constraint.to_ffi();

		let mut field_row = shape_field::SHAPE.allocate();
		shape_field::SHAPE.set_utf8(&mut field_row, shape_field::NAME, &field.name);
		shape_field::SHAPE.set_u8(&mut field_row, shape_field::TYPE, ffi.base_type);
		shape_field::SHAPE.set_u8(&mut field_row, shape_field::CONSTRAINT_TYPE, ffi.constraint_type);
		shape_field::SHAPE.set_u32(&mut field_row, shape_field::CONSTRAINT_P1, ffi.constraint_param1);
		shape_field::SHAPE.set_u32(&mut field_row, shape_field::CONSTRAINT_P2, ffi.constraint_param2);
		shape_field::SHAPE.set_u32(&mut field_row, shape_field::OFFSET, field.offset);
		shape_field::SHAPE.set_u32(&mut field_row, shape_field::SIZE, field.size);
		shape_field::SHAPE.set_u8(&mut field_row, shape_field::ALIGN, field.align);

		cmd.set(&RowShapeFieldKey::encoded(fingerprint, idx as u16), field_row)?;
	}

	Ok(())
}
