// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowShape retrieval from storage.

use reifydb_core::{
	encoded::shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	error::diagnostic::internal::internal,
	key::{
		EncodableKey,
		shape::{RowShapeFieldKey, RowShapeKey},
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::Error,
	value::constraint::{FFITypeConstraint, TypeConstraint},
};
use tracing::{Span, field, instrument};

use super::shape::{shape_field, shape_header};
use crate::Result;

/// Find a shape by its fingerprint.
///
/// Returns None if the shape doesn't exist in storage.
#[instrument(
	name = "shape_store::find",
	level = "trace",
	skip(txn),
	fields(
		fingerprint = ?fingerprint,
		found = field::Empty,
		field_count = field::Empty
	)
)]
pub(crate) fn find_row_shape_by_fingerprint(
	txn: &mut Transaction<'_>,
	fingerprint: RowShapeFingerprint,
) -> Result<Option<RowShape>> {
	// Read shape header
	let header_key = RowShapeKey::encoded(fingerprint);
	let header_entry = match txn.get(&header_key)? {
		Some(entry) => entry,
		None => {
			Span::current().record("found", false);
			Span::current().record("field_count", 0);
			return Ok(None);
		}
	};

	let field_count = shape_header::SHAPE.get_u16(&header_entry.row, shape_header::FIELD_COUNT) as usize;

	let mut fields = Vec::with_capacity(field_count);
	for i in 0..field_count {
		let field_key = RowShapeFieldKey::encoded(fingerprint, i as u16);
		let field_entry = txn.get(&field_key)?.ok_or_else(|| {
			Error(Box::new(internal(format!(
				"RowShape field {} missing for fingerprint {:?}",
				i, fingerprint
			))))
		})?;

		let name = shape_field::SHAPE.get_utf8(&field_entry.row, shape_field::NAME).to_string();
		let base_type = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::TYPE);
		let constraint_type = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::CONSTRAINT_TYPE);
		let constraint_param1 = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::CONSTRAINT_P1);
		let constraint_param2 = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::CONSTRAINT_P2);
		let constraint = TypeConstraint::from_ffi(FFITypeConstraint {
			base_type,
			constraint_type,
			constraint_param1,
			constraint_param2,
		});
		let offset = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::OFFSET);
		let size = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::SIZE);
		let align = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::ALIGN);

		fields.push(RowShapeField {
			name,
			constraint,
			offset,
			size,
			align,
		});
	}

	Span::current().record("found", true);
	Span::current().record("field_count", field_count);
	Ok(Some(RowShape::from_parts(fingerprint, fields)))
}

/// Load all shapes from storage.
///
/// Used during startup to populate the shape registry cache.
#[instrument(
	name = "shape_store::load_all",
	level = "debug",
	skip(rx),
	fields(
		shape_count = field::Empty,
		total_fields = field::Empty
	)
)]
pub fn load_all_row_shapes(rx: &mut Transaction<'_>) -> Result<Vec<RowShape>> {
	// First pass: collect all shape headers (fingerprint, field_count)
	let mut shape_headers: Vec<(RowShapeFingerprint, usize)> = Vec::new();

	{
		let range = RowShapeKey::full_scan();
		let stream = rx.range(range, 1024)?;

		for entry in stream {
			let entry = entry?;

			// Decode the fingerprint from the key
			let shape_key = RowShapeKey::decode(&entry.key)
				.ok_or_else(|| Error(Box::new(internal("Failed to decode shape key"))))?;

			let field_count = shape_header::SHAPE.get_u16(&entry.row, shape_header::FIELD_COUNT) as usize;

			shape_headers.push((shape_key.fingerprint, field_count));
		}
	}

	// Second pass: load fields for each shape
	let mut shapes = Vec::with_capacity(shape_headers.len());

	for (fingerprint, field_count) in shape_headers {
		let mut fields = Vec::with_capacity(field_count);

		for i in 0..field_count {
			let field_key = RowShapeFieldKey::encoded(fingerprint, i as u16);
			let field_entry = rx.get(&field_key)?.ok_or_else(|| {
				Error(Box::new(internal(format!(
					"RowShape field {} missing for fingerprint {:?}",
					i, fingerprint
				))))
			})?;

			let name = shape_field::SHAPE.get_utf8(&field_entry.row, shape_field::NAME).to_string();
			let base_type = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::TYPE);
			let constraint_type = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::CONSTRAINT_TYPE);
			let constraint_param1 =
				shape_field::SHAPE.get_u32(&field_entry.row, shape_field::CONSTRAINT_P1);
			let constraint_param2 =
				shape_field::SHAPE.get_u32(&field_entry.row, shape_field::CONSTRAINT_P2);
			let constraint = TypeConstraint::from_ffi(FFITypeConstraint {
				base_type,
				constraint_type,
				constraint_param1,
				constraint_param2,
			});
			let offset = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::OFFSET);
			let size = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::SIZE);
			let align = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::ALIGN);

			fields.push(RowShapeField {
				name,
				constraint,
				offset,
				size,
				align,
			});
		}

		shapes.push(RowShape::from_parts(fingerprint, fields));
	}

	let total_fields: usize = shapes.iter().map(|s| s.field_count()).sum();
	Span::current().record("shape_count", shapes.len());
	Span::current().record("total_fields", total_fields);

	Ok(shapes)
}
