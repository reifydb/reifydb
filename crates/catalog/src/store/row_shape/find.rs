// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	constraint::{EncodedTypeConstraint, decode_type_constraint},
	row::{
		pod::EncodedPodRow,
		shape::{RowFamily, RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	},
};
use reifydb_core::{
	error::diagnostic::internal::internal,
	key::{
		Key,
		row_shape::{RowShapeFieldKey, RowShapeKey},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::Error;
use tracing::{Span, field, instrument};

use super::shape::{shape_field, shape_header};
use crate::Result;

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
	let header_key = RowShapeKey::encoded(fingerprint);
	let header_entry = match txn.get(&header_key)? {
		Some(entry) => entry,
		None => {
			Span::current().record("found", false);
			Span::current().record("field_count", 0);
			return Ok(None);
		}
	};

	let (family, field_count) = shape_header::decode(EncodedPodRow::view(&header_entry.bytes))?;
	let field_count = field_count as usize;

	let mut fields = Vec::with_capacity(field_count);
	for i in 0..field_count {
		let field_key = RowShapeFieldKey::encoded(fingerprint, i as u16);
		let field_entry = txn.get(&field_key)?.ok_or_else(|| {
			Error(Box::new(internal(format!(
				"RowShape field {} missing for fingerprint {:?}",
				i, fingerprint
			))))
		})?;

		let name = shape_field::SHAPE.get_utf8(&field_entry.bytes, shape_field::NAME).to_string();
		let base_type = shape_field::SHAPE.get::<u8>(&field_entry.bytes, shape_field::TYPE);
		let constraint_type = shape_field::SHAPE.get::<u8>(&field_entry.bytes, shape_field::CONSTRAINT_TYPE);
		let constraint_param1 = shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::CONSTRAINT_P1);
		let constraint_param2 = shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::CONSTRAINT_P2);
		let constraint = decode_type_constraint(&EncodedTypeConstraint {
			base_type,
			constraint_type,
			constraint_param1,
			constraint_param2,
		})
		.expect("invalid persisted type constraint tag");
		let offset = shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::OFFSET);
		let size = shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::SIZE);

		fields.push(RowShapeField {
			name,
			constraint,
			offset,
			size,
		});
	}

	Span::current().record("found", true);
	Span::current().record("field_count", field_count);
	Ok(Some(RowShape::from_parts(family, fingerprint, fields)))
}

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
	let mut shape_headers: Vec<(RowShapeFingerprint, RowFamily, usize)> = Vec::new();

	{
		let range = RowShapeKey::full_scan();
		let stream = rx.range(range, RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;

			let shape_key = RowShapeKey::decode(&entry.key)
				.ok_or_else(|| Error(Box::new(internal("Failed to decode shape key"))))?;

			let (family, field_count) = shape_header::decode(EncodedPodRow::view(&entry.bytes))?;

			shape_headers.push((shape_key.fingerprint, family, field_count as usize));
		}
	}

	let mut shapes = Vec::with_capacity(shape_headers.len());

	for (fingerprint, family, field_count) in shape_headers {
		let mut fields = Vec::with_capacity(field_count);

		for i in 0..field_count {
			let field_key = RowShapeFieldKey::encoded(fingerprint, i as u16);
			let field_entry = rx.get(&field_key)?.ok_or_else(|| {
				Error(Box::new(internal(format!(
					"RowShape field {} missing for fingerprint {:?}",
					i, fingerprint
				))))
			})?;

			let name = shape_field::SHAPE.get_utf8(&field_entry.bytes, shape_field::NAME).to_string();
			let base_type = shape_field::SHAPE.get::<u8>(&field_entry.bytes, shape_field::TYPE);
			let constraint_type =
				shape_field::SHAPE.get::<u8>(&field_entry.bytes, shape_field::CONSTRAINT_TYPE);
			let constraint_param1 =
				shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::CONSTRAINT_P1);
			let constraint_param2 =
				shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::CONSTRAINT_P2);
			let constraint = decode_type_constraint(&EncodedTypeConstraint {
				base_type,
				constraint_type,
				constraint_param1,
				constraint_param2,
			})
			.expect("invalid persisted type constraint tag");
			let offset = shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::OFFSET);
			let size = shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::SIZE);

			fields.push(RowShapeField {
				name,
				constraint,
				offset,
				size,
			});
		}

		shapes.push(RowShape::from_parts(family, fingerprint, fields));
	}

	let total_fields: usize = shapes.iter().map(|s| s.field_count()).sum();
	Span::current().record("shape_count", shapes.len());
	Span::current().record("total_fields", total_fields);

	Ok(shapes)
}

#[cfg(test)]
mod tests {
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_value::value::value_type::ValueType;

	use super::*;
	use crate::store::row_shape::create::create_row_shape;

	fn fields() -> Vec<RowShapeField> {
		vec![RowShapeField::unconstrained("id", ValueType::Uint8)]
	}

	#[test]
	fn a_stored_shape_reloads_under_the_family_it_was_created_with_not_deprecated() {
		let mut txn = create_test_admin_transaction();
		let shape = RowShape::new(RowFamily::Table, fields());
		create_row_shape(&mut Transaction::Admin(&mut txn), &shape).unwrap();

		let loaded = find_row_shape_by_fingerprint(&mut Transaction::Admin(&mut txn), shape.fingerprint())
			.unwrap()
			.unwrap();

		assert_eq!(loaded.family(), RowFamily::Table);
		assert_eq!(loaded.header_size(), shape.header_size());
		assert_eq!(loaded.fingerprint(), shape.fingerprint());
	}

	#[test]
	fn two_families_sharing_a_field_list_reload_as_two_distinct_shapes() {
		let mut txn = create_test_admin_transaction();
		let table = RowShape::new(RowFamily::Table, fields());
		let series = RowShape::new(RowFamily::Series, fields());
		create_row_shape(&mut Transaction::Admin(&mut txn), &table).unwrap();
		create_row_shape(&mut Transaction::Admin(&mut txn), &series).unwrap();

		let loaded_table =
			find_row_shape_by_fingerprint(&mut Transaction::Admin(&mut txn), table.fingerprint())
				.unwrap()
				.unwrap();
		let loaded_series =
			find_row_shape_by_fingerprint(&mut Transaction::Admin(&mut txn), series.fingerprint())
				.unwrap()
				.unwrap();

		assert_eq!(loaded_table.family(), RowFamily::Table);
		assert_eq!(loaded_series.family(), RowFamily::Series);
	}

	#[test]
	fn the_boot_scan_carries_the_family_through_as_well() {
		let mut txn = create_test_admin_transaction();
		let shape = RowShape::new(RowFamily::RingBuffer, fields());
		create_row_shape(&mut Transaction::Admin(&mut txn), &shape).unwrap();

		let loaded = load_all_row_shapes(&mut Transaction::Admin(&mut txn)).unwrap();

		let found = loaded.iter().find(|s| s.fingerprint() == shape.fingerprint()).unwrap();
		assert_eq!(found.family(), RowFamily::RingBuffer);
	}
}
