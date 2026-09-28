// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	envelope::{
		Envelope, EnvelopeBuilder, EnvelopeError, HAS_COMMIT_VERSION, HAS_CREATED_AT, HAS_FINGERPRINT,
		HAS_PARTITION, HAS_TIME, HAS_UPDATED_AT,
	},
	pod::EncodedPodRow,
	shape::fingerprint::RowShapeFingerprint,
};
use reifydb_value::{
	factory::time::at_nanos,
	value::{datetime::DateTime, partition::Partition},
};

const ALL_FLAGS: u8 = HAS_CREATED_AT | HAS_UPDATED_AT | HAS_TIME | HAS_FINGERPRINT;

const FINGERPRINT: RowShapeFingerprint = RowShapeFingerprint::new(0x0123_4567_89ab_cdef);

fn created_at() -> DateTime {
	at_nanos(11)
}

fn updated_at() -> DateTime {
	at_nanos(22)
}

fn time() -> DateTime {
	at_nanos(33)
}

fn builder_for(flags: u8) -> EnvelopeBuilder {
	// every field carries a distinct value, so a mis-computed offset reads a neighbour instead of erroring
	let mut builder = EnvelopeBuilder::new();
	if flags & HAS_CREATED_AT != 0 {
		builder = builder.created_at(created_at());
	}
	if flags & HAS_UPDATED_AT != 0 {
		builder = builder.updated_at(updated_at());
	}
	if flags & HAS_TIME != 0 {
		builder = builder.time(time());
	}
	if flags & HAS_FINGERPRINT != 0 {
		builder = builder.fingerprint(FINGERPRINT);
	}
	builder
}

#[test]
fn every_flag_combination_places_each_field_where_its_flags_imply() {
	// fields sit in flag-bit order with no padding, so an absent one must shift every later field left by 8
	for flags in 0..=ALL_FLAGS {
		let row = builder_for(flags).build(b"payload");
		let envelope = Envelope::try_view(&row).expect("the builder always writes a complete header");

		assert_eq!(envelope.flags(), flags, "flags {flags:#06b}");
		assert_eq!(envelope.header_size(), 1 + 8 * flags.count_ones() as usize, "flags {flags:#06b}");

		assert_eq!(envelope.created_at(), (flags & HAS_CREATED_AT != 0).then(created_at), "flags {flags:#06b}");
		assert_eq!(envelope.updated_at(), (flags & HAS_UPDATED_AT != 0).then(updated_at), "flags {flags:#06b}");
		assert_eq!(envelope.time(), (flags & HAS_TIME != 0).then(time), "flags {flags:#06b}");
		assert_eq!(
			envelope.fingerprint(),
			(flags & HAS_FINGERPRINT != 0).then_some(FINGERPRINT),
			"flags {flags:#06b}"
		);

		assert_eq!(envelope.body(), b"payload", "flags {flags:#06b}");
		assert_eq!(row.len(), envelope.header_size() + 7, "flags {flags:#06b}");
	}
}

#[test]
fn an_empty_body_leaves_the_header_intact_under_every_flag_combination() {
	// a header-only row is legal state, so body() must yield an empty slice and never underflow the range
	for flags in 0..=ALL_FLAGS {
		let row = builder_for(flags).build(&[]);
		let envelope = Envelope::try_view(&row).expect("a header-only row is complete");

		assert!(envelope.body().is_empty(), "flags {flags:#06b}");
		assert_eq!(row.len(), envelope.header_size(), "flags {flags:#06b}");
		assert_eq!(envelope.created_at(), (flags & HAS_CREATED_AT != 0).then(created_at));
		assert_eq!(envelope.fingerprint(), (flags & HAS_FINGERPRINT != 0).then_some(FINGERPRINT));
	}
}

#[test]
fn try_view_rejects_a_buffer_shorter_than_the_header_its_own_flags_declare() {
	// without this guard a truncated row resolves its stamps out of the body, or indexes past the allocation
	let full = builder_for(ALL_FLAGS).build(b"payload").as_slice().to_vec();
	let required = 1 + 8 * ALL_FLAGS.count_ones() as usize;
	assert_eq!(required, 33);

	for len in 1..required {
		let short = EncodedPodRow::new(&full[..len]);
		assert_eq!(
			Envelope::try_view(&short).unwrap_err(),
			EnvelopeError::Truncated {
				len,
				required,
			}
		);
	}

	let complete = EncodedPodRow::new(&full[..required]);
	assert!(Envelope::try_view(&complete).is_ok(), "the exact header length must be accepted");
}

#[test]
fn try_view_rejects_an_empty_buffer() {
	// an empty row carries no flags byte, so deciding the header length from offset 0 would panic
	let empty = EncodedPodRow::new(&[]);

	assert_eq!(
		Envelope::try_view(&empty).unwrap_err(),
		EnvelopeError::Truncated {
			len: 0,
			required: 1,
		}
	);
}

#[test]
fn the_join_shape_costs_the_same_seventeen_bytes_the_hand_rolled_header_did() {
	// the hand-rolled join row is 17 bytes, so a fingerprint plus one instant must never grow past that
	let timeless = builder_for(HAS_FINGERPRINT | HAS_CREATED_AT).build(b"row");
	let timeless = Envelope::try_view(&timeless).expect("header is complete");
	assert_eq!(timeless.header_size(), 17);
	assert_eq!(timeless.fingerprint(), Some(FINGERPRINT));
	assert_eq!(timeless.created_at(), Some(created_at()));
	assert_eq!(timeless.time(), None);

	let timed = builder_for(HAS_FINGERPRINT | HAS_TIME).build(b"row");
	let timed = Envelope::try_view(&timed).expect("header is complete");
	assert_eq!(timed.header_size(), 17);
	assert_eq!(timed.fingerprint(), Some(FINGERPRINT));
	assert_eq!(timed.time(), Some(time()));
}

#[test]
fn the_take_shape_costs_twenty_five_bytes_with_a_time_and_seventeen_without() {
	// take is the one user whose header grows, and anything above 25 bytes costs more than the plan budgeted
	let timed = builder_for(HAS_CREATED_AT | HAS_UPDATED_AT | HAS_TIME).build(b"row");
	let timed = Envelope::try_view(&timed).expect("header is complete");
	assert_eq!(timed.header_size(), 25);
	assert_eq!(timed.created_at(), Some(created_at()));
	assert_eq!(timed.updated_at(), Some(updated_at()));
	assert_eq!(timed.time(), Some(time()));

	let timeless = builder_for(HAS_CREATED_AT | HAS_UPDATED_AT).build(b"row");
	let timeless = Envelope::try_view(&timeless).expect("header is complete");
	assert_eq!(timeless.header_size(), 17);
	assert_eq!(timeless.created_at(), Some(created_at()));
	assert_eq!(timeless.updated_at(), Some(updated_at()));
	assert_eq!(timeless.time(), None);
}

#[test]
fn a_max_datetime_in_a_present_field_reads_back_as_present() {
	// the operator header spelled absence as DateTime::MAX, so presence must now ride the flag and never a value
	let row = EnvelopeBuilder::new()
		.created_at(DateTime::MAX)
		.updated_at(DateTime::MAX)
		.time(DateTime::MAX)
		.build(b"row");
	let envelope = Envelope::try_view(&row).expect("header is complete");

	assert_eq!(envelope.created_at(), Some(DateTime::MAX));
	assert_eq!(envelope.updated_at(), Some(DateTime::MAX));
	assert_eq!(envelope.time(), Some(DateTime::MAX));

	let absent = EnvelopeBuilder::new().build(b"row");
	let absent = Envelope::try_view(&absent).expect("a bare flags byte is a complete header");
	assert_eq!(absent.created_at(), None);
	assert_eq!(absent.time(), None);
}

#[test]
fn a_body_round_trips_byte_for_byte() {
	// the envelope must stay transparent to its payload, since any byte it eats or shifts is state lost on flush
	let payload: Vec<u8> = (0..=255).collect();

	let bare = EnvelopeBuilder::new().build(&payload);
	let bare = Envelope::try_view(&bare).expect("a bare flags byte is a complete header");
	assert_eq!(bare.header_size(), 1);
	assert_eq!(bare.body(), payload.as_slice());

	let stamped = builder_for(ALL_FLAGS).build(&payload);
	let stamped = Envelope::try_view(&stamped).expect("header is complete");
	assert_eq!(stamped.body(), payload.as_slice());
	assert_eq!(stamped.created_at(), Some(created_at()));
}

#[test]
fn a_body_that_opens_with_flag_bytes_is_never_read_as_a_header() {
	// the leading flags byte alone fixes the header, so payload bytes that look like flags must not extend it
	let payload = [0xFFu8; 40];

	let row = EnvelopeBuilder::new().created_at(created_at()).build(&payload);
	let envelope = Envelope::try_view(&row).expect("header is complete");

	assert_eq!(envelope.header_size(), 9);
	assert_eq!(envelope.body(), payload.as_slice());
	assert_eq!(envelope.created_at(), Some(created_at()));
}

const STAMPED_FLAGS: u8 = ALL_FLAGS | HAS_COMMIT_VERSION | HAS_PARTITION;

const COMMIT_VERSION: u64 = 0x1122_3344_5566_7788;

const PARTITION: Partition = Partition(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

fn stamped_builder_for(flags: u8) -> EnvelopeBuilder {
	// the commit version and partition carry values no other field holds, so a shifted read is caught
	let mut builder = builder_for(flags & ALL_FLAGS);
	if flags & HAS_COMMIT_VERSION != 0 {
		builder = builder.commit_version(COMMIT_VERSION);
	}
	if flags & HAS_PARTITION != 0 {
		builder = builder.partition(PARTITION);
	}
	builder
}

#[test]
fn every_combination_with_commit_version_and_partition_places_each_field_where_its_flags_imply() {
	// the partition is 16 bytes, so a header sized at 8 per flag would read the body as its upper half
	assert_eq!(stamped_builder_for(STAMPED_FLAGS).build(&[]).len(), 57);
	for flags in 0..=STAMPED_FLAGS {
		let row = stamped_builder_for(flags).build(b"payload");
		let envelope = Envelope::try_view(&row).expect("the builder always writes a complete header");
		let partition = if flags & HAS_PARTITION != 0 {
			16
		} else {
			0
		};

		assert_eq!(envelope.flags(), flags, "flags {flags:#08b}");
		assert_eq!(
			envelope.header_size(),
			1 + 8 * (flags & !HAS_PARTITION).count_ones() as usize + partition,
			"flags {flags:#08b}"
		);

		assert_eq!(envelope.created_at(), (flags & HAS_CREATED_AT != 0).then(created_at), "flags {flags:#08b}");
		assert_eq!(envelope.updated_at(), (flags & HAS_UPDATED_AT != 0).then(updated_at), "flags {flags:#08b}");
		assert_eq!(envelope.time(), (flags & HAS_TIME != 0).then(time), "flags {flags:#08b}");
		assert_eq!(
			envelope.fingerprint(),
			(flags & HAS_FINGERPRINT != 0).then_some(FINGERPRINT),
			"flags {flags:#08b}"
		);
		assert_eq!(
			envelope.commit_version(),
			(flags & HAS_COMMIT_VERSION != 0).then_some(COMMIT_VERSION),
			"flags {flags:#08b}"
		);
		assert_eq!(
			envelope.partition(),
			(flags & HAS_PARTITION != 0).then_some(PARTITION),
			"flags {flags:#08b}"
		);

		assert_eq!(envelope.body(), b"payload", "flags {flags:#08b}");
		assert_eq!(row.len(), envelope.header_size() + 7, "flags {flags:#08b}");
	}
}

#[test]
fn try_view_rejects_a_buffer_cut_inside_the_partition_field() {
	// the partition's upper 8 bytes are header too, so a row cut there must never read body bytes as a stamp
	let full = stamped_builder_for(STAMPED_FLAGS).build(b"payload").as_slice().to_vec();
	let required = 57;

	for len in required - 16..required {
		let short = EncodedPodRow::new(&full[..len]);
		assert_eq!(
			Envelope::try_view(&short).unwrap_err(),
			EnvelopeError::Truncated {
				len,
				required,
			}
		);
	}

	let complete = EncodedPodRow::new(&full[..required]);
	let envelope = Envelope::try_view(&complete).expect("the exact header length must be accepted");
	assert_eq!(envelope.partition(), Some(PARTITION));
	assert!(envelope.body().is_empty());
}
