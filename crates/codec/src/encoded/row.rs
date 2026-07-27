// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Deref;

use reifydb_value::util::cowvec::CowVec;
use rkyv::{
	Archive, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
	rancor::Fallible,
	ser::{Allocator, Writer},
	vec::{ArchivedVec, VecResolver},
};
use serde::{Deserialize, Serialize};

use crate::encoded::shape::fingerprint::RowShapeFingerprint;

pub const SHAPE_HEADER_SIZE: usize = 32;

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

	pub(crate) fn set_valid(&mut self, index: usize, valid: bool) {
		let byte = SHAPE_HEADER_SIZE + index / 8;
		let bit = index % 8;
		if valid {
			self.0.make_mut()[byte] |= 1 << bit;
		} else {
			self.0.make_mut()[byte] &= !(1 << bit);
		}
	}

	#[inline]
	pub fn fingerprint(&self) -> RowShapeFingerprint {
		let bytes: [u8; 8] = self.0[0..8].try_into().unwrap();
		RowShapeFingerprint::from_le_bytes(bytes)
	}

	pub fn set_fingerprint(&mut self, fingerprint: RowShapeFingerprint) {
		self.0.make_mut()[0..8].copy_from_slice(&fingerprint.to_le_bytes());
	}

	#[inline]
	pub fn created_at_nanos(&self) -> u64 {
		let bytes: [u8; 8] = self.0[8..16].try_into().unwrap();
		u64::from_le_bytes(bytes)
	}

	#[inline]
	pub fn updated_at_nanos(&self) -> u64 {
		let bytes: [u8; 8] = self.0[16..24].try_into().unwrap();
		u64::from_le_bytes(bytes)
	}

	pub fn set_timestamps(&mut self, created_at_nanos: u64, updated_at_nanos: u64) {
		let buf = self.0.make_mut();
		buf[8..16].copy_from_slice(&created_at_nanos.to_le_bytes());
		buf[16..24].copy_from_slice(&updated_at_nanos.to_le_bytes());
	}

	#[inline]
	pub fn time_nanos(&self) -> u64 {
		let bytes: [u8; 8] = self.0[24..32].try_into().unwrap();
		u64::from_le_bytes(bytes)
	}

	pub fn set_time_nanos(&mut self, time_nanos: u64) {
		self.0.make_mut()[24..32].copy_from_slice(&time_nanos.to_le_bytes());
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::value_type::ValueType;

	use crate::encoded::shape::{RowShape, RowShapeField, SHAPE_HEADER_SIZE};

	fn shape(field_count: usize) -> RowShape {
		RowShape::new(
			(0..field_count)
				.map(|i| RowShapeField::unconstrained(format!("f{i}"), ValueType::Uint8))
				.collect(),
		)
	}

	#[test]
	// Intent: #time occupies its own header slot. The three stamps answer three different
	// questions (when the DB learned this / last touched it / when it happened), so a write to
	// one must never be observable in another. Mutation: point time_nanos at [16..24) so it
	// overlaps updated_at, and the reads below start returning each other's values.
	fn time_round_trips_independently_of_created_at_and_updated_at() {
		let mut row = shape(1).allocate();

		row.set_timestamps(11, 22);
		row.set_time_nanos(33);

		assert_eq!(row.created_at_nanos(), 11);
		assert_eq!(row.updated_at_nanos(), 22);
		assert_eq!(row.time_nanos(), 33);

		row.set_time_nanos(44);
		assert_eq!(row.created_at_nanos(), 11, "writing #time must not disturb created_at");
		assert_eq!(row.updated_at_nanos(), 22, "writing #time must not disturb updated_at");
		assert_eq!(row.time_nanos(), 44);

		row.set_timestamps(55, 66);
		assert_eq!(row.time_nanos(), 44, "writing the wall stamps must not disturb #time");
	}

	#[test]
	// Intent: #time has a created_at-like lifecycle, not an updated_at-like one. set_timestamps
	// is the verbatim-rewrite path used by the seal flush; it refreshes updated_at on bytes it
	// copies through. #time describes when the event happened, so a local rewrite must leave it
	// alone or every downstream retention decision drifts to wall clock. Mutation: widen
	// set_timestamps to also stamp [24..32) and this fails.
	fn time_survives_a_verbatim_rewrite_that_refreshes_updated_at() {
		let mut row = shape(1).allocate();
		row.set_timestamps(7, 7);
		row.set_time_nanos(1_000);

		let created_at = row.created_at_nanos();
		row.set_timestamps(created_at, 99);

		assert_eq!(row.created_at_nanos(), 7);
		assert_eq!(row.updated_at_nanos(), 99, "the rewrite refreshes updated_at");
		assert_eq!(row.time_nanos(), 1_000, "#time is propagated, never re-stamped locally");
	}

	#[test]
	// Intent: the four header slots must fit inside SHAPE_HEADER_SIZE, so that no header write
	// reaches the bitvec or the static section. A round-trip alone cannot prove this: the
	// accessors and the layout both derive from the constant, so they move together and stay
	// self-consistent even when the constant is wrong. What actually breaks is the boundary, and
	// only a test that names both sides of it can see that. #time occupies [24..32), so a
	// SHAPE_HEADER_SIZE of 24 puts the bitvec at 24 and set_time_nanos silently erases it -
	// which is how a stale constant corrupts rows without failing a single round-trip.
	// Mutation: set either SHAPE_HEADER_SIZE definition back to 24 and this fails.
	fn the_header_slots_end_before_the_bitvec_begins() {
		assert!(
			SHAPE_HEADER_SIZE >= 32,
			"#time occupies [24..32), so the bitvec must not start before 32 (got {SHAPE_HEADER_SIZE})"
		);

		let shape = shape(9);
		let mut row = shape.allocate();

		for i in 0..9 {
			shape.set_u64(&mut row, i, (i as u64 + 1) * 1_000);
		}
		row.set_timestamps(1, 2);
		row.set_time_nanos(u64::MAX);

		for i in 0..9 {
			assert_eq!(shape.get_u64(&row, i), (i as u64 + 1) * 1_000, "field {i} misread");
			assert!(row.is_defined(i), "field {i} lost its definedness bit to a header write");
		}
		assert_eq!(row.created_at_nanos(), 1);
		assert_eq!(row.updated_at_nanos(), 2);
		assert_eq!(row.time_nanos(), u64::MAX);
	}

	#[test]
	// Intent: #time lives outside user field space, so it costs no definedness bit and does not
	// shift user field indices. A row is always timed (D1), which is exactly why it must not be
	// representable as an absent field. Mutation: allocate #time as field 0 of the shape and the
	// bit-to-field mapping below slides by one.
	fn time_consumes_no_definedness_bit() {
		let shape = shape(9);
		let mut row = shape.allocate();
		row.set_time_nanos(u64::MAX);

		for i in 0..9 {
			assert!(!row.is_defined(i), "field {i} must start undefined regardless of #time");
		}

		shape.set_u64(&mut row, 3, 42u64);
		assert!(row.is_defined(3), "bit 3 maps to user field 3, not to a system slot");
		for i in (0..9).filter(|i| *i != 3) {
			assert!(!row.is_defined(i), "defining field 3 must not define field {i}");
		}

		assert_eq!(row.time_nanos(), u64::MAX, "#time is unaffected by definedness writes");
		assert_eq!(shape.bitvec_size(), 2, "9 fields still need exactly 2 bitvec bytes");
		assert_eq!(shape.data_offset(), SHAPE_HEADER_SIZE + 2);
	}
}
