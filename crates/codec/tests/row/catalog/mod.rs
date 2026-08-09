// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{CATALOG_HEADER_SIZE, EncodedBytes, RowBuilder},
	catalog::{CatalogError, EncodedCatalogRow},
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::{util::cowvec::CowVec, value::value_type::ValueType};

fn shape() -> RowShape {
	RowShape::new(
		RowFamily::Catalog,
		vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
		],
	)
}

fn row(id: u64, name: &str) -> EncodedBytes {
	let shape = shape();
	let mut builder = shape.allocate_catalog();
	shape.set::<u64>(&mut builder, 0, id);
	shape.set_utf8(&mut builder, 1, name);
	builder.freeze_bytes()
}

#[test]
fn a_viewed_row_reports_the_fingerprint_the_shape_stamped() {
	// Reading any offset but 0 resolves field bytes as a shape id and decodes the row against a foreign layout.
	let shape = shape();
	let bytes = row(7, "catalog");

	let view = EncodedCatalogRow::view(&bytes);

	assert_eq!(view.fingerprint(), shape.fingerprint());
}

#[test]
fn definedness_is_read_at_the_catalog_header_not_the_source_header() {
	// Probing byte 33 on a catalog row answers from field data, never from a validity bit.
	let shape = shape();
	let mut builder = shape.allocate_catalog();
	shape.set::<u64>(&mut builder, 0, 7);
	shape.set_none(&mut builder, 1);
	let bytes = builder.freeze_bytes();

	let view = EncodedCatalogRow::view(&bytes);

	assert!(view.is_defined(0), "field 0 was written and must read back defined");
	assert!(!view.is_defined(1), "field 1 was set none and must read back undefined");
	assert_eq!(view.is_defined(0), shape.is_defined(&bytes, 0));
	assert_eq!(view.is_defined(1), shape.is_defined(&bytes, 1));
}

#[test]
fn the_body_begins_exactly_where_the_fingerprint_ends() {
	// A body starting one byte off shifts every field, so the bitvec reads as a value and the last field overruns.
	let bytes = row(7, "catalog");
	let view = EncodedCatalogRow::view(&bytes);

	assert_eq!(view.body().len(), bytes.len() - CATALOG_HEADER_SIZE);
	assert_eq!(view.body(), &bytes.as_slice()[CATALOG_HEADER_SIZE..]);
	assert_eq!(view.len(), bytes.len(), "len is the whole row, not the body");
}

#[test]
fn a_constructed_row_round_trips_its_fingerprint_and_body() {
	// Without write and read agreeing on the header, new() silently produces rows nothing can resolve.
	let fingerprint = shape().fingerprint();
	let body = [1u8, 2, 3, 4];

	let built = EncodedCatalogRow::new(&body, fingerprint);

	assert_eq!(built.fingerprint(), fingerprint);
	assert_eq!(built.body(), &body);
	assert_eq!(built.len(), CATALOG_HEADER_SIZE + body.len());
}

#[test]
fn conversion_to_bytes_and_back_preserves_every_byte() {
	// EncodedBytes crosses the storage boundary, so a conversion that reorders or pads corrupts every catalog row.
	let original = row(7, "catalog");

	let converted: EncodedBytes =
		EncodedCatalogRow::try_from(original.clone()).expect("a full catalog row is long enough").into();

	assert_eq!(converted.as_slice(), original.as_slice());
}

#[test]
fn bytes_too_short_to_hold_a_fingerprint_are_rejected() {
	// Accepting a truncated buffer defers the failure to a slice index, turning a decode error into a panic.
	for len in 0..CATALOG_HEADER_SIZE {
		let truncated = EncodedBytes(CowVec::new(vec![0u8; len]));

		assert_eq!(
			EncodedCatalogRow::try_from(truncated),
			Err(CatalogError::Truncated {
				len
			}),
			"a {len}-byte buffer cannot carry an 8-byte fingerprint"
		);
	}

	let exact = EncodedBytes(CowVec::new(vec![0u8; CATALOG_HEADER_SIZE]));
	assert!(EncodedCatalogRow::try_from(exact).is_ok(), "a header with an empty body is still a valid row");
}

#[test]
fn set_fingerprint_rewrites_the_header_without_touching_the_body() {
	// Restamping a row's shape must never disturb its values, or a shape migration corrupts what it migrates.
	let bytes = row(7, "catalog");
	let body_before = bytes.as_slice()[CATALOG_HEADER_SIZE..].to_vec();
	let mut catalog_row = EncodedCatalogRow::try_from(bytes).expect("a full catalog row is long enough");

	let replacement =
		RowShape::new(RowFamily::Catalog, vec![RowShapeField::unconstrained("other", ValueType::Uint8)])
			.fingerprint();
	catalog_row.set_fingerprint(replacement);

	assert_eq!(catalog_row.fingerprint(), replacement);
	assert_eq!(catalog_row.body(), body_before.as_slice());
}
