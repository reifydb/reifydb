// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crc32fast::Hasher;
use reifydb_value::{reifydb_assertions, value::datetime::DateTime};

use crate::log::{LogIndex, LogVersion, RecordKind, Term};

pub const HEADER_BYTES: usize = 48;

pub const MIN_LENGTH: u32 = 44;

pub const RESERVED: u32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
	pub version: LogVersion,
	pub index: LogIndex,
	pub term: Term,
	pub timestamp: DateTime,
	pub kind: RecordKind,
	pub payload: Vec<u8>,
}

impl Record {
	pub fn new(
		version: LogVersion,
		index: LogIndex,
		term: Term,
		timestamp: DateTime,
		kind: RecordKind,
		payload: Vec<u8>,
	) -> Self {
		Self {
			version,
			index,
			term,
			timestamp,
			kind,
			payload,
		}
	}

	pub fn encoded_len(&self) -> usize {
		HEADER_BYTES + self.payload.len()
	}

	pub fn encode(&self) -> Vec<u8> {
		let length = MIN_LENGTH as usize + self.payload.len();
		reifydb_assertions! {
			assert!(
				length <= u32::MAX as usize,
				"a payload of {} bytes overflows the four byte length field, which wraps silently and \
				 frames the record at a length no scan can follow (payload limit={})",
				self.payload.len(),
				u32::MAX as usize - MIN_LENGTH as usize
			);
		}
		let mut out = Vec::with_capacity(HEADER_BYTES + self.payload.len());
		out.extend_from_slice(&(length as u32).to_le_bytes());
		out.extend_from_slice(&0u32.to_le_bytes());
		out.extend_from_slice(&self.version.as_u64().to_le_bytes());
		out.extend_from_slice(&self.index.as_u64().to_le_bytes());
		out.extend_from_slice(&self.term.as_u64().to_le_bytes());
		out.extend_from_slice(&self.timestamp.to_bits().to_le_bytes());
		out.extend_from_slice(&self.kind.as_u32().to_le_bytes());
		out.extend_from_slice(&RESERVED.to_le_bytes());
		out.extend_from_slice(&self.payload);
		let checksum = checksum(&out[8..]);
		out[4..8].copy_from_slice(&checksum.to_le_bytes());
		out
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
	pub length: u32,
	pub checksum: u32,
	pub version: LogVersion,
	pub index: LogIndex,
	pub term: Term,
	pub timestamp: DateTime,
	pub kind: RecordKind,
	pub reserved: u32,
}

impl Header {
	pub fn decode(buf: &[u8; HEADER_BYTES]) -> Self {
		Self {
			length: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
			checksum: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
			version: LogVersion::new(u64::from_le_bytes(buf[8..16].try_into().unwrap())),
			index: LogIndex::new(u64::from_le_bytes(buf[16..24].try_into().unwrap())),
			term: Term::new(u64::from_le_bytes(buf[24..32].try_into().unwrap())),
			timestamp: DateTime::from_bits(u64::from_le_bytes(buf[32..40].try_into().unwrap())),
			kind: RecordKind::new(u32::from_le_bytes(buf[40..44].try_into().unwrap())),
			reserved: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
		}
	}

	pub fn is_end(&self) -> bool {
		self.length == 0
	}

	pub fn payload_len(&self) -> Option<usize> {
		if self.length < MIN_LENGTH {
			return None;
		}
		Some((self.length - MIN_LENGTH) as usize)
	}

	pub fn verify(&self, payload: &[u8]) -> bool {
		let mut hasher = Hasher::new();
		hasher.update(&self.version.as_u64().to_le_bytes());
		hasher.update(&self.index.as_u64().to_le_bytes());
		hasher.update(&self.term.as_u64().to_le_bytes());
		hasher.update(&self.timestamp.to_bits().to_le_bytes());
		hasher.update(&self.kind.as_u32().to_le_bytes());
		hasher.update(&self.reserved.to_le_bytes());
		hasher.update(payload);
		hasher.finalize() == self.checksum
	}

	pub fn into_record(self, payload: Vec<u8>) -> Record {
		Record {
			version: self.version,
			index: self.index,
			term: self.term,
			timestamp: self.timestamp,
			kind: self.kind,
			payload,
		}
	}
}

fn checksum(bytes: &[u8]) -> u32 {
	let mut hasher = Hasher::new();
	hasher.update(bytes);
	hasher.finalize()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn header_of(bytes: &[u8]) -> Header {
		Header::decode(bytes[..HEADER_BYTES].try_into().unwrap())
	}

	fn record(version: u64, index: u64, term: u64, timestamp: u64, kind: u32, payload: Vec<u8>) -> Record {
		Record::new(
			LogVersion::new(version),
			LogIndex::new(index),
			Term::new(term),
			DateTime::from_bits(timestamp),
			RecordKind::new(kind),
			payload,
		)
	}

	#[test]
	fn encode_lays_the_fields_out_at_the_documented_offsets() {
		// The offsets are the on disk format; moving one silently makes every existing
		// segment unreadable, so they are asserted numerically rather than via decode.
		let bytes = record(
			0x0102030405060708,
			0x2122232425262728,
			0x3132333435363738,
			0x1112131415161718,
			0x41424344,
			vec![0xaa, 0xbb],
		)
		.encode();

		assert_eq!(bytes.len(), HEADER_BYTES + 2);
		assert_eq!(&bytes[0..4], &46u32.to_le_bytes());
		assert_eq!(&bytes[8..16], &0x0102030405060708u64.to_le_bytes());
		assert_eq!(&bytes[16..24], &0x2122232425262728u64.to_le_bytes());
		assert_eq!(&bytes[24..32], &0x3132333435363738u64.to_le_bytes());
		assert_eq!(&bytes[32..40], &0x1112131415161718u64.to_le_bytes());
		assert_eq!(&bytes[40..44], &0x41424344u32.to_le_bytes());
		assert_eq!(&bytes[44..48], &0u32.to_le_bytes());
		assert_eq!(&bytes[48..50], &[0xaa, 0xbb]);
	}

	#[test]
	fn length_counts_the_bytes_after_itself() {
		// length must exclude its own four bytes; counting them would make every scan
		// advance four bytes too far and land mid record.
		let record = record(1, 2, 3, 4, 0, vec![0u8; 100]);
		let bytes = record.encode();
		let header = header_of(&bytes);

		assert_eq!(header.length as usize, bytes.len() - 4);
		assert_eq!(header.payload_len(), Some(100));
	}

	#[test]
	fn a_roundtrip_returns_the_payload_byte_identical() {
		let payload: Vec<u8> = (0..=255u8).collect();
		let record = record(7, 9, 11, 13, 1, payload.clone());
		let bytes = record.encode();
		let header = header_of(&bytes);

		assert!(header.verify(&bytes[HEADER_BYTES..]));
		assert_eq!(header.into_record(bytes[HEADER_BYTES..].to_vec()), record);
	}

	#[test]
	fn a_roundtrip_returns_the_index_term_and_kind() {
		// The three raft fields have no other reader yet, so nothing but this test stops
		// them being dropped on the floor between encode and decode.
		let header = header_of(&record(500, 7, 3, 1234, 1, vec![0x11, 0x22]).encode());

		assert_eq!(header.version, LogVersion::new(500));
		assert_eq!(header.index, LogIndex::new(7));
		assert_eq!(header.term, Term::new(3));
		assert_eq!(header.timestamp, DateTime::from_bits(1234));
		assert_eq!(header.kind, RecordKind::new(1));
		assert_eq!(header.reserved, RESERVED);
	}

	#[test]
	fn an_empty_payload_is_a_valid_record_and_not_a_terminator() {
		// length zero terminates a scan, so a record carrying no payload must still
		// report a non zero length or an empty append would truncate the segment.
		let bytes = record(3, 4, 5, 6, 0, Vec::new()).encode();
		let header = header_of(&bytes);

		assert_eq!(header.length, MIN_LENGTH);
		assert!(!header.is_end());
		assert_eq!(header.payload_len(), Some(0));
		assert!(header.verify(&[]));
	}

	#[test]
	fn a_zeroed_header_reads_as_the_end_of_the_written_region() {
		// Segments are preallocated with zeros, so the first unwritten byte must decode
		// as a terminator rather than as a record of length zero.
		let header = Header::decode(&[0u8; HEADER_BYTES]);

		assert!(header.is_end());
	}

	#[test]
	fn a_flipped_payload_bit_fails_verification() {
		let mut bytes = record(1, 2, 3, 4, 0, vec![0x55; 64]).encode();
		bytes[HEADER_BYTES + 30] ^= 0x01;
		let header = header_of(&bytes);

		assert!(!header.verify(&bytes[HEADER_BYTES..]));
	}

	#[test]
	fn a_flipped_version_bit_fails_verification() {
		// The checksum must cover the version, otherwise a torn header is mistaken for a
		// valid record sitting at a plausible version.
		let mut bytes = record(1, 2, 3, 4, 0, vec![0x55; 8]).encode();
		bytes[8] ^= 0x01;
		let header = header_of(&bytes);

		assert!(!header.verify(&bytes[HEADER_BYTES..]));
	}

	#[test]
	fn a_flipped_timestamp_bit_fails_verification() {
		let mut bytes = record(1, 2, 3, 4, 0, vec![0x55; 8]).encode();
		bytes[32] ^= 0x01;
		let header = header_of(&bytes);

		assert!(!header.verify(&bytes[HEADER_BYTES..]));
	}

	#[test]
	fn a_flipped_bit_anywhere_past_the_length_fails_verification() {
		// Everything from offset 8 on is inside the checksum, including the raft fields
		// and the reserved padding; a gap there lets a torn header pass as a real record.
		for byte in 8..HEADER_BYTES {
			let mut bytes = record(1, 2, 3, 4, 1, vec![0x55; 8]).encode();
			bytes[byte] ^= 0x01;
			let header = header_of(&bytes);

			assert!(!header.verify(&bytes[HEADER_BYTES..]), "byte {byte} is outside the checksum");
		}
	}

	#[test]
	fn a_length_below_the_minimum_has_no_payload_length() {
		// Garbage that decodes to a short length must be rejected rather than wrapping
		// around to a huge payload length in the subtraction below it.
		for length in 1..MIN_LENGTH {
			let mut buf = [0u8; HEADER_BYTES];
			buf[0..4].copy_from_slice(&length.to_le_bytes());

			assert_eq!(Header::decode(&buf).payload_len(), None, "length {length}");
		}
	}

	#[test]
	fn two_records_differing_only_in_payload_have_different_checksums() {
		let a = header_of(&record(1, 2, 3, 4, 0, vec![0x00]).encode());
		let b = header_of(&record(1, 2, 3, 4, 0, vec![0x01]).encode());

		assert_ne!(a.checksum, b.checksum);
	}

	#[test]
	fn two_records_differing_only_in_a_raft_field_have_different_checksums() {
		// index, term and kind must each move the checksum, otherwise a record rewritten
		// under a different leader verifies against the old header.
		let base = header_of(&record(1, 2, 3, 4, 0, vec![0x00]).encode());

		assert_ne!(base.checksum, header_of(&record(1, 9, 3, 4, 0, vec![0x00]).encode()).checksum);
		assert_ne!(base.checksum, header_of(&record(1, 2, 9, 4, 0, vec![0x00]).encode()).checksum);
		assert_ne!(base.checksum, header_of(&record(1, 2, 3, 4, 9, vec![0x00]).encode()).checksum);
	}

	#[test]
	fn encoded_len_matches_what_encode_produces() {
		// The appender reserves space from encoded_len before it encodes; a mismatch
		// writes past the end of a preallocated segment.
		for size in [0usize, 1, 511, 512, 513, 4096] {
			let record = record(1, 2, 3, 4, 0, vec![0u8; size]);

			assert_eq!(record.encoded_len(), record.encode().len());
		}
	}
}
