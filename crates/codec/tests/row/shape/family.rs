// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{
		CATALOG_HEADER_SIZE, QUEUE_ATTEMPT_HEADER_SIZE, QUEUE_DEDUPLICATION_HEADER_SIZE, QUEUE_HEADER_SIZE,
		SHAPE_HEADER_SIZE, write_timestamps,
	},
	operator::OPERATOR_HEADER_SIZE,
	pod::POD_HEADER_SIZE,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::value::{datetime::DateTime, value_type::ValueType};

const SOURCE_FAMILIES: [RowFamily; 6] = [
	RowFamily::Table,
	RowFamily::Series,
	RowFamily::RingBuffer,
	RowFamily::Queue,
	RowFamily::QueueAttempt,
	RowFamily::QueueDeduplication,
];

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
fn a_table_shape_keeps_the_full_source_row_header() {
	// Storage rows still carry created_at/updated_at/time/flags; shrinking this reinterprets every stored row.
	let shape = RowShape::new(RowFamily::Table, fields());

	assert_eq!(shape.header_size(), SHAPE_HEADER_SIZE);
	assert_eq!(shape.header_size(), 33);
	assert_eq!(shape.fields()[0].offset as usize, SHAPE_HEADER_SIZE + shape.bitvec_size());
}

#[test]
fn the_catalog_family_saves_exactly_the_dead_header_bytes_on_every_row() {
	// Drift here means a family gained a header field or the catalog family stopped being fingerprint-only.
	let catalog = RowShape::new(RowFamily::Catalog, fields());
	let table = RowShape::new(RowFamily::Table, fields());

	assert_eq!(table.total_static_size() - catalog.total_static_size(), 25);
}

#[test]
fn the_family_participates_in_the_fingerprint_so_two_layouts_cannot_collide() {
	// The fingerprint is the shape registry's only key, so a shared one would read a catalog row at offset 33.
	let catalog = RowShape::new(RowFamily::Catalog, fields());
	let table = RowShape::new(RowFamily::Table, fields());

	assert_ne!(catalog.header_size(), table.header_size());
	assert_ne!(catalog.fingerprint(), table.fingerprint());
}

#[test]
fn the_source_families_never_share_a_fingerprint_for_the_same_columns() {
	// The fingerprint is the registry's only key, so a shared one lets a series row resolve to a table's shape.
	for (i, left) in SOURCE_FAMILIES.iter().enumerate() {
		for right in SOURCE_FAMILIES.iter().skip(i + 1) {
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
fn every_family_tag_keeps_the_byte_it_was_assigned_and_round_trips_through_from_u8() {
	// The tag seeds the fingerprint and is a stored shape header's only content, so a silent renumber reinterprets
	// every persisted row under the wrong family.
	let expected = [
		(RowFamily::Catalog, 0x01u8),
		(RowFamily::Pod, 0x02),
		(RowFamily::Table, 0x03),
		(RowFamily::Series, 0x04),
		(RowFamily::RingBuffer, 0x05),
		(RowFamily::Queue, 0x06),
		(RowFamily::Operator, 0x07),
		(RowFamily::QueueAttempt, 0x08),
		(RowFamily::QueueDeduplication, 0x09),
	];

	for (family, tag) in expected {
		assert_eq!(family as u8, tag, "{family:?} must keep tag {tag:#04x}");
		assert_eq!(RowFamily::from_u8(tag), Some(family), "tag {tag:#04x} must decode back to {family:?}");
	}

	assert_eq!(RowFamily::from_u8(0x00), None, "zero is not a family and must never decode");
	assert_eq!(RowFamily::from_u8(0x0A), None, "the range is dense, so the byte past the last tag is unknown");
}

#[test]
fn a_catalog_row_reads_back_the_values_it_wrote() {
	// A shape allocating an 8-byte header but probing the bitvec at 33 would read a field byte as a validity bit.
	let shape = RowShape::new(RowFamily::Catalog, fields());
	let mut row = shape.allocate_catalog();

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

#[test]
fn every_family_declares_the_header_width_its_own_accessors_read() {
	// header_size feeds compute_layout, so a family wired to the wrong constant shifts every field it owns.
	assert_eq!(RowFamily::Catalog.header_size(), CATALOG_HEADER_SIZE);
	assert_eq!(RowFamily::Operator.header_size(), OPERATOR_HEADER_SIZE);
	assert_eq!(RowFamily::Pod.header_size(), POD_HEADER_SIZE);
	assert_eq!(RowFamily::Table.header_size(), SHAPE_HEADER_SIZE);
	assert_eq!(RowFamily::Series.header_size(), SHAPE_HEADER_SIZE);
	assert_eq!(RowFamily::RingBuffer.header_size(), SHAPE_HEADER_SIZE);
	assert_eq!(RowFamily::Queue.header_size(), QUEUE_HEADER_SIZE);
	assert_eq!(RowFamily::QueueAttempt.header_size(), QUEUE_ATTEMPT_HEADER_SIZE);
	assert_eq!(RowFamily::QueueDeduplication.header_size(), QUEUE_DEDUPLICATION_HEADER_SIZE);
}

#[test]
fn the_nine_header_widths_are_the_widths_every_stored_row_was_written_under() {
	// Checking only against the constants is circular, so these literals are the on-disk contract itself.
	assert_eq!(RowFamily::Pod.header_size(), 0, "a pod row is payload from offset zero");
	assert_eq!(RowFamily::Catalog.header_size(), 8, "a catalog header is the fingerprint and nothing else");
	assert_eq!(RowFamily::Operator.header_size(), 8, "the operator row is nothing but the instant it belongs to");
	assert_eq!(RowFamily::Table.header_size(), 33);
	assert_eq!(RowFamily::Series.header_size(), 33);
	assert_eq!(RowFamily::RingBuffer.header_size(), 33);
	assert_eq!(RowFamily::Queue.header_size(), 41, "the source header plus not_before");
	assert_eq!(RowFamily::QueueAttempt.header_size(), 43, "the source header plus outcome, lost and finished_at");
	assert_eq!(RowFamily::QueueDeduplication.header_size(), 49, "the source header plus row_number and expires_at");
}

#[test]
fn the_stamped_families_read_updated_at_from_one_shared_offset() {
	// Queue, attempt and deduplication widen the header after the stamps, so their reads must not shift with it.
	let mut row = vec![0u8; QUEUE_DEDUPLICATION_HEADER_SIZE];
	write_timestamps(&mut row, DateTime::from_millis(11), DateTime::from_millis(22));

	for family in SOURCE_FAMILIES {
		assert_eq!(family.updated_at(&row), DateTime::from_millis(22), "{family:?} misreads updated_at");
	}
}

#[test]
#[should_panic(expected = "Catalog rows carry no updated_at")]
fn a_catalog_row_has_no_updated_at_to_read() {
	// A catalog header is eight fingerprint bytes, so a stamp read there would hand back field data as an instant.
	RowFamily::Catalog.updated_at(&[0u8; QUEUE_DEDUPLICATION_HEADER_SIZE]);
}

#[test]
#[should_panic(expected = "Operator rows carry no updated_at")]
fn an_operator_row_keeps_its_stamps_outside_the_shared_window() {
	// Operator packs created_at at offset zero, so the shared reader must refuse rather than read the wrong slot.
	RowFamily::Operator.updated_at(&[0u8; QUEUE_DEDUPLICATION_HEADER_SIZE]);
}

#[test]
#[should_panic(expected = "Pod rows carry no updated_at")]
fn a_pod_row_has_no_updated_at_to_read() {
	// A pod row is payload from offset zero, so a stamp read would return interned entry bytes as a time.
	RowFamily::Pod.updated_at(&[0u8; QUEUE_DEDUPLICATION_HEADER_SIZE]);
}
