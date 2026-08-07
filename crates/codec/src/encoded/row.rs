// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Deref;

use reifydb_value::{encoding::LeBytes, util::cowvec::CowVec, value::datetime::DateTime};
use rkyv::{
	Archive, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
	rancor::Fallible,
	ser::{Allocator, Writer},
	vec::{ArchivedVec, VecResolver},
};
use serde::{Deserialize, Serialize};

use crate::encoded::shape::fingerprint::RowShapeFingerprint;

const FINGERPRINT_SIZE: usize = 8;
const CREATED_AT_OFFSET: usize = FINGERPRINT_SIZE;
const UPDATED_AT_OFFSET: usize = CREATED_AT_OFFSET + DateTime::ENCODED_SIZE;
const TIME_OFFSET: usize = UPDATED_AT_OFFSET + DateTime::ENCODED_SIZE;
const FLAGS_OFFSET: usize = TIME_OFFSET + DateTime::ENCODED_SIZE;

pub const SHAPE_HEADER_SIZE: usize = FLAGS_OFFSET + 1;

const HAS_TIME: u8 = 1 << 0;

pub type EncodedRowIter = Box<dyn EncodedRowIterator>;

pub trait EncodedRowIterator: Iterator<Item = EncodedRow> {}

impl<I: Iterator<Item = EncodedRow>> EncodedRowIterator for I {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncodedRow(pub CowVec<u8>);

impl Deref for EncodedRow {
	type Target = CowVec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Archive for EncodedRow {
	type Archived = ArchivedVec<u8>;
	type Resolver = VecResolver;

	fn resolve(&self, resolver: Self::Resolver, out: Place<Self::Archived>) {
		ArchivedVec::resolve_from_len(self.0.len(), resolver, out);
	}
}

impl<S: Fallible + Writer + Allocator + ?Sized> RkyvSerialize<S> for EncodedRow {
	fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
		ArchivedVec::serialize_from_slice(self.0.as_slice(), serializer)
	}
}

impl<D: Fallible + ?Sized> RkyvDeserialize<EncodedRow, D> for ArchivedVec<u8> {
	fn deserialize(&self, _: &mut D) -> Result<EncodedRow, D::Error> {
		Ok(EncodedRow(CowVec::new(self.as_slice().to_vec())))
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRowBuilder(Vec<u8>);

impl EncodedRowBuilder {
	pub(crate) fn zeroed(size: usize) -> Self {
		Self(vec![0u8; size])
	}

	pub fn as_slice(&self) -> &[u8] {
		&self.0
	}

	pub fn as_mut_slice(&mut self) -> &mut [u8] {
		&mut self.0
	}

	pub(crate) fn vec_mut(&mut self) -> &mut Vec<u8> {
		&mut self.0
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn extend_from_slice(&mut self, bytes: &[u8]) {
		self.0.extend_from_slice(bytes);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined(&self.0, index)
	}

	pub(crate) fn set_valid(&mut self, index: usize, valid: bool) {
		let byte = SHAPE_HEADER_SIZE + index / 8;
		let bit = index % 8;
		if valid {
			self.0[byte] |= 1 << bit;
		} else {
			self.0[byte] &= !(1 << bit);
		}
	}

	#[inline]
	pub fn fingerprint(&self) -> RowShapeFingerprint {
		read_fingerprint(&self.0)
	}

	pub fn set_fingerprint(&mut self, fingerprint: RowShapeFingerprint) {
		self.0[0..FINGERPRINT_SIZE].copy_from_slice(&fingerprint.to_le_bytes());
	}

	#[inline]
	pub fn created_at(&self) -> DateTime {
		read_stamp(&self.0, CREATED_AT_OFFSET)
	}

	#[inline]
	pub fn updated_at(&self) -> DateTime {
		read_stamp(&self.0, UPDATED_AT_OFFSET)
	}

	#[inline]
	pub fn time(&self) -> Option<DateTime> {
		read_time(&self.0)
	}

	pub fn set_timestamps(&mut self, created_at: DateTime, updated_at: DateTime) {
		self.0[CREATED_AT_OFFSET..CREATED_AT_OFFSET + DateTime::ENCODED_SIZE]
			.copy_from_slice(&created_at.to_le_bytes());
		self.0[UPDATED_AT_OFFSET..UPDATED_AT_OFFSET + DateTime::ENCODED_SIZE]
			.copy_from_slice(&updated_at.to_le_bytes());
	}

	pub fn set_time(&mut self, time: DateTime) {
		self.0[TIME_OFFSET..TIME_OFFSET + DateTime::ENCODED_SIZE].copy_from_slice(&time.to_le_bytes());
		self.0[FLAGS_OFFSET] |= HAS_TIME;
	}

	pub fn freeze(self) -> EncodedRow {
		EncodedRow(CowVec::new(self.0))
	}
}

impl Deref for EncodedRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<EncodedRowBuilder> for EncodedRow {
	fn from(builder: EncodedRowBuilder) -> Self {
		builder.freeze()
	}
}

#[inline]
pub fn read_defined(buf: &[u8], index: usize) -> bool {
	let byte = SHAPE_HEADER_SIZE + index / 8;
	let bit = index % 8;
	(buf[byte] & (1 << bit)) != 0
}

#[inline]
fn read_fingerprint(buf: &[u8]) -> RowShapeFingerprint {
	let bytes: [u8; FINGERPRINT_SIZE] = buf[0..FINGERPRINT_SIZE].try_into().unwrap();
	RowShapeFingerprint::from_le_bytes(bytes)
}

#[inline]
fn read_stamp(buf: &[u8], offset: usize) -> DateTime {
	DateTime::from_le_bytes(buf[offset..offset + DateTime::ENCODED_SIZE].try_into().unwrap())
}

#[inline]
fn read_time(buf: &[u8]) -> Option<DateTime> {
	(buf[FLAGS_OFFSET] & HAS_TIME != 0).then(|| read_stamp(buf, TIME_OFFSET))
}

impl EncodedRow {
	pub fn thaw(self) -> EncodedRowBuilder {
		EncodedRowBuilder(self.0.into_inner())
	}
}

impl EncodedRow {
	pub fn make_mut(&mut self) -> &mut [u8] {
		self.0.make_mut()
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		let byte = SHAPE_HEADER_SIZE + index / 8;
		let bit = index % 8;
		(self.0[byte] & (1 << bit)) != 0
	}

	#[inline]
	pub fn fingerprint(&self) -> RowShapeFingerprint {
		let bytes: [u8; FINGERPRINT_SIZE] = self.0[0..FINGERPRINT_SIZE].try_into().unwrap();
		RowShapeFingerprint::from_le_bytes(bytes)
	}

	pub fn set_fingerprint(&mut self, fingerprint: RowShapeFingerprint) {
		self.0.make_mut()[0..FINGERPRINT_SIZE].copy_from_slice(&fingerprint.to_le_bytes());
	}

	#[inline]
	fn stamp(&self, offset: usize) -> DateTime {
		DateTime::from_le_bytes(self.0[offset..offset + DateTime::ENCODED_SIZE].try_into().unwrap())
	}

	#[inline]
	pub fn created_at(&self) -> DateTime {
		self.stamp(CREATED_AT_OFFSET)
	}

	#[inline]
	pub fn updated_at(&self) -> DateTime {
		self.stamp(UPDATED_AT_OFFSET)
	}

	pub fn set_timestamps(&mut self, created_at: DateTime, updated_at: DateTime) {
		let buf = self.0.make_mut();
		buf[CREATED_AT_OFFSET..CREATED_AT_OFFSET + DateTime::ENCODED_SIZE]
			.copy_from_slice(&created_at.to_le_bytes());
		buf[UPDATED_AT_OFFSET..UPDATED_AT_OFFSET + DateTime::ENCODED_SIZE]
			.copy_from_slice(&updated_at.to_le_bytes());
	}

	#[inline]
	pub fn time(&self) -> Option<DateTime> {
		read_time(&self.0)
	}

	pub fn set_time(&mut self, time: DateTime) {
		let buf = self.0.make_mut();
		buf[TIME_OFFSET..TIME_OFFSET + DateTime::ENCODED_SIZE].copy_from_slice(&time.to_le_bytes());
		buf[FLAGS_OFFSET] |= HAS_TIME;
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::{
		encoding::LeBytes,
		factory::time::at_nanos,
		value::{datetime::DateTime, value_type::ValueType},
	};

	use crate::encoded::{
		row::{
			CREATED_AT_OFFSET, FINGERPRINT_SIZE, FLAGS_OFFSET, HAS_TIME, SHAPE_HEADER_SIZE, TIME_OFFSET,
			UPDATED_AT_OFFSET,
		},
		shape::{RowShape, RowShapeField},
	};

	fn shape(field_count: usize) -> RowShape {
		RowShape::new(
			(0..field_count)
				.map(|i| RowShapeField::unconstrained(format!("f{i}"), ValueType::Uint8))
				.collect(),
		)
	}

	#[test]
	fn time_round_trips_independently_of_created_at_and_updated_at() {
		// The three stamps answer different questions (when the DB learned a row, last touched
		// it, when the event happened), so overlapping slots would make one readable as another.
		let mut row = shape(1).allocate();

		row.set_timestamps(at_nanos(11), at_nanos(22));
		row.set_time(at_nanos(33));

		assert_eq!(row.created_at(), at_nanos(11));
		assert_eq!(row.updated_at(), at_nanos(22));
		assert_eq!(row.time(), Some(at_nanos(33)));

		row.set_time(at_nanos(44));
		assert_eq!(row.created_at(), at_nanos(11), "writing #time must not disturb created_at");
		assert_eq!(row.updated_at(), at_nanos(22), "writing #time must not disturb updated_at");
		assert_eq!(row.time(), Some(at_nanos(44)));

		row.set_timestamps(at_nanos(55), at_nanos(66));
		assert_eq!(row.time(), Some(at_nanos(44)), "writing the wall stamps must not disturb #time");
	}

	#[test]
	fn time_survives_a_verbatim_rewrite_that_refreshes_updated_at() {
		// set_timestamps is the seal flush's verbatim-rewrite path. #time describes when the
		// event happened, so re-stamping it locally would drift retention to wall clock.
		let mut row = shape(1).allocate();
		row.set_timestamps(at_nanos(7), at_nanos(7));
		row.set_time(at_nanos(1_000));

		let created_at = row.created_at();
		row.set_timestamps(created_at, at_nanos(99));

		assert_eq!(row.created_at(), at_nanos(7));
		assert_eq!(row.updated_at(), at_nanos(99), "the rewrite refreshes updated_at");
		assert_eq!(row.time(), Some(at_nanos(1_000)), "#time is propagated, never re-stamped locally");
	}

	#[test]
	fn the_header_slots_end_before_the_bitvec_begins() {
		// Accessors and layout derive from the same constants, so a round trip stays
		// self-consistent even when the arithmetic is wrong. Only the boundary breaks: the
		// slots must tile SHAPE_HEADER_SIZE exactly, leaving the bitvec and fields untouched.
		assert_eq!(CREATED_AT_OFFSET, FINGERPRINT_SIZE, "the first stamp starts where the fingerprint ends");
		assert_eq!(UPDATED_AT_OFFSET, CREATED_AT_OFFSET + DateTime::ENCODED_SIZE);
		assert_eq!(TIME_OFFSET, UPDATED_AT_OFFSET + DateTime::ENCODED_SIZE);
		assert_eq!(
			FLAGS_OFFSET,
			TIME_OFFSET + DateTime::ENCODED_SIZE,
			"the flags byte sits after the last stamp, whatever a DateTime is worth"
		);
		assert_eq!(SHAPE_HEADER_SIZE, FLAGS_OFFSET + 1, "the bitvec must start after the flags byte");

		let shape = shape(9);
		let mut row = shape.allocate();

		for i in 0..9 {
			shape.set::<u64>(&mut row, i, (i as u64 + 1) * 1_000);
		}
		row.set_timestamps(at_nanos(1), at_nanos(2));
		row.set_time(DateTime::MAX);

		for i in 0..9 {
			assert_eq!(shape.get::<u64>(&row, i), (i as u64 + 1) * 1_000, "field {i} misread");
			assert!(row.is_defined(i), "field {i} lost its definedness bit to a header write");
		}
		assert_eq!(row.created_at(), at_nanos(1));
		assert_eq!(row.updated_at(), at_nanos(2));
		assert_eq!(row.time(), Some(DateTime::MAX));
	}

	#[test]
	fn a_row_that_was_never_stamped_carries_no_time() {
		// A zeroed slot is indistinguishable from a stamp of zero, so without a presence bit a
		// time-less object cannot withhold #time and downstream resolves the ambiguity by
		// substituting a wall clock.
		let shape = shape(3);
		let mut row = shape.allocate();

		assert_eq!(row.time(), None, "a freshly allocated row carries no #time");

		shape.set::<u64>(&mut row, 0, 7u64);
		row.set_timestamps(at_nanos(1), at_nanos(2));

		assert_eq!(row.time(), None, "writing fields and wall stamps must not conjure a #time");
		assert_eq!(row.clone().freeze().time(), None, "absence must survive the freeze");
	}

	#[test]
	fn an_epoch_stamp_is_a_real_time_not_an_absence() {
		// Presence is decided by the flag, never by the value, so the epoch stays an ordinary
		// coordinate. Treating it as a sentinel would make a row genuinely dated 1970 unreadable.
		let mut row = shape(1).allocate();
		row.set_time(DateTime::EPOCH);

		assert_eq!(row.time(), Some(DateTime::EPOCH));
		assert_ne!(row.time(), None, "an explicitly stamped epoch is present, not absent");
	}

	#[test]
	fn stamping_time_leaves_every_other_flag_bit_clear() {
		// Bits 1..7 are unassigned. Holding them at zero is what lets a future flag be introduced
		// without a format migration: every row written today already reads as "that flag is off".
		let shape = shape(4);
		let mut row = shape.allocate();

		assert_eq!(row.0[FLAGS_OFFSET], 0, "allocation must leave the flags byte clear");

		row.set_time(at_nanos(5));
		assert_eq!(row.0[FLAGS_OFFSET], HAS_TIME, "set_time must touch only its own bit");

		row.set_timestamps(at_nanos(1), at_nanos(2));
		row.set_fingerprint(shape.fingerprint());
		shape.set::<u64>(&mut row, 3, 42u64);
		assert_eq!(row.0[FLAGS_OFFSET], HAS_TIME, "no other header or field write may reach the flags byte");
	}

	#[test]
	fn the_flags_byte_is_not_the_first_bitvec_byte() {
		// Both live at the tail of the header and are bit-addressed, so an off-by-one in
		// SHAPE_HEADER_SIZE would silently alias field 0's definedness onto HAS_TIME.
		let shape = shape(8);
		let mut row = shape.allocate();

		shape.set::<u64>(&mut row, 0, 1u64);
		assert!(row.is_defined(0));
		assert_eq!(row.time(), None, "defining field 0 must not set HAS_TIME");

		let mut row = shape.allocate();
		row.set_time(at_nanos(9));
		for i in 0..8 {
			assert!(!row.is_defined(i), "stamping #time must not define field {i}");
		}
	}

	#[test]
	fn time_consumes_no_definedness_bit() {
		// #time is absent-representable, but through the header flag rather than the field bitvec: it
		// lives outside user field space, costing no definedness bit and shifting no field index.
		let shape = shape(9);
		let mut row = shape.allocate();
		row.set_time(DateTime::MAX);

		for i in 0..9 {
			assert!(!row.is_defined(i), "field {i} must start undefined regardless of #time");
		}

		shape.set::<u64>(&mut row, 3, 42u64);
		assert!(row.is_defined(3), "bit 3 maps to user field 3, not to a system slot");
		for i in (0..9).filter(|i| *i != 3) {
			assert!(!row.is_defined(i), "defining field 3 must not define field {i}");
		}

		assert_eq!(row.time(), Some(DateTime::MAX), "#time is unaffected by definedness writes");
		assert_eq!(shape.bitvec_size(), 2, "9 fields still need exactly 2 bitvec bytes");
		assert_eq!(shape.data_offset(), SHAPE_HEADER_SIZE + 2);
	}

	#[test]
	fn a_stamp_slot_holds_exactly_one_datetime_encoding() {
		// Stamps go through DateTime's own byte form, not a local u64 cast, so widening
		// DateTime moves the header with it instead of truncating into an old-width slot.
		let mut row = shape(1).allocate();
		let stamp = at_nanos(0x0102_0304_0506_0708);
		row.set_time(stamp);

		assert_eq!(&row.0[TIME_OFFSET..TIME_OFFSET + DateTime::ENCODED_SIZE], &stamp.to_le_bytes());
		assert_eq!(DateTime::from_le_bytes(stamp.to_le_bytes()), stamp);
	}
}
