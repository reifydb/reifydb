// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{constraint::encode_type_constraint_info, row::shape::RowShape};
use reifydb_core::key::row::{RowShapeFieldKey, RowShapeKey};
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
	txn.set(vec![(
		RowShapeKey::new(fingerprint).into(),
		shape_header::encode(shape.family(), shape.field_count() as u16).into_bytes(),
	)])?;

	for (idx, field) in shape.fields().iter().enumerate() {
		let mut type_constraint = Vec::new();
		encode_type_constraint_info(&field.constraint, &mut type_constraint)
			.unwrap_or_else(|error| panic!("row shape field {} cannot be encoded: {error}", field.name));

		let mut field_row = shape_field::SHAPE.allocate_catalog();
		shape_field::SHAPE.set_utf8(&mut field_row, shape_field::NAME, &field.name);
		shape_field::SHAPE.set_blob_from_slice(&mut field_row, shape_field::TYPE_CONSTRAINT, &type_constraint);
		shape_field::SHAPE.set::<u32>(&mut field_row, shape_field::OFFSET, field.offset);
		shape_field::SHAPE.set::<u32>(&mut field_row, shape_field::SIZE, field.size);

		txn.set(vec![(RowShapeFieldKey::new(fingerprint, idx as u16).into(), field_row.freeze().into())])?;
	}

	Ok(())
}
