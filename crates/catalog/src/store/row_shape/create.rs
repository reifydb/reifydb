// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{constraint::encode_type_constraint, row::shape::RowShape};
use reifydb_core::key::row_shape::{RowShapeFieldKey, RowShapeKey};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::reifydb_assertions;
use tracing::instrument;

use super::shape::{shape_field, shape_header};
use crate::Result;

#[instrument(
	name = "shape_store::create",
	level = "debug",
	skip(txn, shape),
	fields(fingerprint = ?shape.fingerprint(), field_count = shape.field_count())
)]
pub(crate) fn create_row_shape(txn: &mut Transaction<'_>, shape: &RowShape) -> Result<()> {
	let fingerprint = shape.fingerprint();

	reifydb_assertions! {
		assert!(
			shape.field_count() <= u16::MAX as usize,
			"shape field_count exceeds u16::MAX so the header FIELD_COUNT cell silently truncates and readers reconstruct a schema with the wrong number of fields (field_count={})",
			shape.field_count()
		);
	}
	txn.set(
		&RowShapeKey::encoded(fingerprint),
		shape_header::encode(shape.family(), shape.field_count() as u16).into_bytes(),
	)?;

	for (idx, field) in shape.fields().iter().enumerate() {
		let extern_c = encode_type_constraint(&field.constraint).expect("constraint exceeds tag capacity");

		let mut field_row = shape_field::SHAPE.allocate_catalog();
		shape_field::SHAPE.set_utf8(&mut field_row, shape_field::NAME, &field.name);
		shape_field::SHAPE.set::<u8>(&mut field_row, shape_field::TYPE, extern_c.base_type);
		shape_field::SHAPE.set::<u8>(&mut field_row, shape_field::CONSTRAINT_TYPE, extern_c.constraint_type);
		shape_field::SHAPE.set::<u32>(&mut field_row, shape_field::CONSTRAINT_P1, extern_c.constraint_param1);
		shape_field::SHAPE.set::<u32>(&mut field_row, shape_field::CONSTRAINT_P2, extern_c.constraint_param2);
		shape_field::SHAPE.set::<u32>(&mut field_row, shape_field::OFFSET, field.offset);
		shape_field::SHAPE.set::<u32>(&mut field_row, shape_field::SIZE, field.size);

		txn.set(&RowShapeFieldKey::encoded(fingerprint, idx as u16), field_row.freeze())?;
	}

	Ok(())
}
