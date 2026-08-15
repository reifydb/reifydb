// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{QUEUE_DEDUPLICATION_HEADER_SIZE, RowBuilder, SHAPE_HEADER_SIZE},
	queue_deduplication::EncodedQueueDeduplicationRow,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::{
	encoding::LeBytes,
	value::{datetime::DateTime, row_number::RowNumber, value_type::ValueType},
};

fn shape() -> RowShape {
	RowShape::new(RowFamily::QueueDeduplication, vec![])
}

#[test]
fn the_deduplication_header_is_the_source_header_plus_row_number_and_expires_at() {
	// Any other width silently reinterprets both facts in every stored deduplication record.
	assert_eq!(QUEUE_DEDUPLICATION_HEADER_SIZE, SHAPE_HEADER_SIZE + RowNumber::ENCODED_SIZE + DateTime::ENCODED_SIZE);
	assert_eq!(QUEUE_DEDUPLICATION_HEADER_SIZE, 49);
	assert_eq!(shape().header_size(), QUEUE_DEDUPLICATION_HEADER_SIZE);
}

#[test]
fn a_field_less_shape_carries_no_bitvec_and_ends_at_the_header() {
	// A stray bitvec byte would push the record past its header and make the width checks disagree.
	let shape = shape();

	assert_eq!(shape.bitvec_size(), 0);
	assert_eq!(shape.data_offset(), QUEUE_DEDUPLICATION_HEADER_SIZE);
	assert_eq!(shape.allocate_queue_deduplication().freeze().as_slice().len(), QUEUE_DEDUPLICATION_HEADER_SIZE);
}

#[test]
fn both_header_facts_survive_a_freeze() {
	// A slot lost on freeze would point the duplicate at the wrong row or expire it immediately.
	let mut row = shape().allocate_queue_deduplication();
	row.set_row_number(RowNumber(4_294_967_297));
	row.set_expires_at(DateTime::from_nanos(1_234_567_890));

	let frozen = row.freeze();

	assert_eq!(frozen.row_number(), RowNumber(4_294_967_297));
	assert_eq!(frozen.expires_at(), DateTime::from_nanos(1_234_567_890));
}

#[test]
fn the_row_number_and_the_expiry_occupy_disjoint_slots() {
	// Overlapping offsets would let a write to one fact corrupt the other.
	let mut row = shape().allocate_queue_deduplication();
	row.set_row_number(RowNumber(u64::MAX));
	row.set_expires_at(DateTime::from_nanos(0));

	assert_eq!(row.row_number(), RowNumber(u64::MAX));
	assert_eq!(row.expires_at(), DateTime::from_nanos(0));

	row.set_row_number(RowNumber(0));
	row.set_expires_at(DateTime::from_nanos(i64::MAX as u64));

	assert_eq!(row.row_number(), RowNumber(0));
	assert_eq!(row.expires_at(), DateTime::from_nanos(i64::MAX as u64));
}

#[test]
fn a_view_over_raw_bytes_reads_the_same_header_facts() {
	// A view that disagreed with the builder would resolve stored duplicates differently from written ones.
	let mut row = shape().allocate_queue_deduplication();
	row.set_row_number(RowNumber(77));
	row.set_expires_at(DateTime::from_nanos(7));
	let bytes = row.freeze_bytes();

	let viewed = EncodedQueueDeduplicationRow::view(&bytes);

	assert_eq!(viewed.row_number(), RowNumber(77));
	assert_eq!(viewed.expires_at(), DateTime::from_nanos(7));
}

#[test]
fn the_header_facts_are_always_present_and_never_absent() {
	// These carry no flag bit, so a fresh record must read as a concrete zero rather than as unset.
	let row = shape().allocate_queue_deduplication().freeze();

	assert_eq!(row.row_number(), RowNumber(0));
	assert_eq!(row.expires_at(), DateTime::from_nanos(0));
}

#[test]
fn a_freshly_allocated_row_already_carries_the_shape_it_was_allocated_from() {
	// allocate reaches this family only through its catch-all arm, so narrowing that arm would leave the row unresolvable.
	let shape = shape();

	let row = shape.allocate_queue_deduplication().freeze();

	assert_eq!(row.fingerprint(), shape.fingerprint());
	assert_ne!(row.fingerprint(), RowShape::new(RowFamily::Queue, vec![]).fingerprint());
}

#[test]
fn the_wall_clock_stamps_survive_a_freeze_and_stay_disjoint() {
	// Retention sweeps on updated_at, so one shared stamp slot makes every record look either immortal or already expired.
	let mut row = shape().allocate_queue_deduplication();
	row.set_timestamps(DateTime::from_nanos(11), DateTime::from_nanos(22));

	let frozen = row.freeze();

	assert_eq!(frozen.created_at(), DateTime::from_nanos(11));
	assert_eq!(frozen.updated_at(), DateTime::from_nanos(22));
}

#[test]
fn a_row_handed_to_storage_and_read_back_is_the_same_row() {
	// Every write leaves through into_bytes and every read arrives through From, so an asymmetry there loses the claim.
	let mut row = shape().allocate_queue_deduplication();
	row.set_row_number(RowNumber(31));
	row.set_expires_at(DateTime::from_nanos(7));
	let original = row.freeze();

	let restored = EncodedQueueDeduplicationRow::from(original.clone().into_bytes());

	assert_eq!(restored, original);
	assert_eq!(restored.row_number(), RowNumber(31));
	assert_eq!(restored.expires_at(), DateTime::from_nanos(7));
}

#[test]
fn thawing_a_claim_extends_its_expiry_without_moving_the_row_number() {
	// The row type has no setter at all, so this is the only path a sweeper has to push an expiry out.
	let mut row = shape().allocate_queue_deduplication();
	row.set_timestamps(DateTime::from_nanos(1), DateTime::from_nanos(1));
	row.set_row_number(RowNumber(4_294_967_297));
	row.set_expires_at(DateTime::from_nanos(1_000));
	let frozen = row.freeze();

	let mut thawed = frozen.thaw();
	assert_eq!(thawed.row_number(), RowNumber(4_294_967_297), "the claim must survive the round trip");
	thawed.set_expires_at(DateTime::from_nanos(2_000));
	let refrozen = thawed.freeze();

	assert_eq!(refrozen.row_number(), RowNumber(4_294_967_297), "extending a claim must never repoint it");
	assert_eq!(refrozen.expires_at(), DateTime::from_nanos(2_000));
	assert_eq!(refrozen.created_at(), DateTime::from_nanos(1));
	assert_eq!(refrozen.as_slice().len(), QUEUE_DEDUPLICATION_HEADER_SIZE, "a body-less record must not grow");
}

#[test]
#[should_panic(expected = "allocate_queue_deduplication on a shape of another family")]
fn a_shape_of_another_family_cannot_allocate_a_deduplication_row() {
	// Allocating across families must panic, otherwise the declared shape and the layout disagree.
	RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("key", ValueType::Utf8)])
		.allocate_queue_deduplication();
}
