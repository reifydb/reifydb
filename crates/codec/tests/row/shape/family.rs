// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{CATALOG_HEADER_SIZE, SHAPE_HEADER_SIZE},
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::value::value_type::ValueType;

fn fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("id", ValueType::Uint8),
		RowShapeField::unconstrained("name", ValueType::Utf8),
	]
}

#[test]
fn a_catalog_shape_starts_its_bitvec_directly_after_the_fingerprint() {
	// A catalog row never reads created_at/updated_at/time/flags, so those 25 bytes must never be reserved.
	let shape = RowShape::new(RowFamily::Catalog, fields());

	assert_eq!(shape.header_size(), CATALOG_HEADER_SIZE);
	assert_eq!(shape.header_size(), 8, "a catalog header is the fingerprint and nothing else");
	assert_eq!(
		shape.data_offset(),
		CATALOG_HEADER_SIZE + shape.bitvec_size(),
		"fields must begin right after the bitvec, which itself begins right after the fingerprint"
	);
	assert_eq!(shape.fields()[0].offset as usize, CATALOG_HEADER_SIZE + shape.bitvec_size());
}

#[test]
fn a_deprecated_shape_keeps_the_full_source_row_header() {
	// Storage rows still carry created_at/updated_at/time/flags; shrinking this reinterprets every stored row.
	let shape = RowShape::new(RowFamily::Deprecated, fields());

	assert_eq!(shape.header_size(), SHAPE_HEADER_SIZE);
	assert_eq!(shape.header_size(), 33);
	assert_eq!(shape.fields()[0].offset as usize, SHAPE_HEADER_SIZE + shape.bitvec_size());
}

#[test]
fn the_catalog_family_saves_exactly_the_dead_header_bytes_on_every_row() {
	// Drift here means a family gained a header field or the catalog family stopped being fingerprint-only.
	let catalog = RowShape::new(RowFamily::Catalog, fields());
	let deprecated = RowShape::new(RowFamily::Deprecated, fields());

	assert_eq!(deprecated.total_static_size() - catalog.total_static_size(), 25);
}

#[test]
fn the_family_participates_in_the_fingerprint_so_two_layouts_cannot_collide() {
	// The fingerprint is the shape registry's only key, so a shared one would read a catalog row at offset 33.
	let catalog = RowShape::new(RowFamily::Catalog, fields());
	let deprecated = RowShape::new(RowFamily::Deprecated, fields());

	assert_ne!(catalog.header_size(), deprecated.header_size());
	assert_ne!(catalog.fingerprint(), deprecated.fingerprint());
}

#[test]
fn the_four_source_families_never_share_a_fingerprint_for_the_same_columns() {
	// The fingerprint is the registry's only key, so a shared one lets a series row resolve to a table's shape.
	let families = [RowFamily::Table, RowFamily::Series, RowFamily::RingBuffer, RowFamily::Queue];

	for (i, left) in families.iter().enumerate() {
		for right in families.iter().skip(i + 1) {
			assert_ne!(
				RowShape::new(*left, fields()).fingerprint(),
				RowShape::new(*right, fields()).fingerprint(),
				"{left:?} and {right:?} collide"
			);
		}
	}
}

#[test]
fn the_three_thirty_three_byte_source_families_lay_their_fields_out_identically() {
	// Table, series and ring buffer differ only in type, so a header-width drift in one silently reframes its rows.
	let table = RowShape::new(RowFamily::Table, fields());
	let series = RowShape::new(RowFamily::Series, fields());
	let ringbuffer = RowShape::new(RowFamily::RingBuffer, fields());

	for shape in [&series, &ringbuffer] {
		assert_eq!(shape.header_size(), table.header_size());
		assert_eq!(shape.data_offset(), table.data_offset());
		assert_eq!(shape.fields()[0].offset, table.fields()[0].offset);
		assert_eq!(shape.total_static_size(), table.total_static_size());
	}
}

#[test]
fn a_catalog_row_reads_back_the_values_it_wrote() {
	// A shape allocating an 8-byte header but probing the bitvec at 33 would read a field byte as a validity bit.
	let shape = RowShape::new(RowFamily::Catalog, fields());
	let mut row = shape.allocate();

	shape.set::<u64>(&mut row, 0, 7);
	shape.set_utf8(&mut row, 1, "catalog");

	assert!(shape.is_defined(&row, 0));
	assert!(shape.is_defined(&row, 1));
	assert_eq!(shape.get::<u64>(&row, 0), 7);
	assert_eq!(shape.get_utf8(&row, 1), "catalog");

	shape.set_none(&mut row, 1);
	assert!(!shape.is_defined(&row, 1));
	assert!(shape.is_defined(&row, 0), "clearing one field must not disturb another's validity bit");
}
