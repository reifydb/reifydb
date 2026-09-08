// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crc32fast::Hasher;
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

pub const META_BYTES: usize = 48;

pub const MAGIC: u32 = u32::from_le_bytes(*b"RMET");

pub const FORMAT_VERSION: u32 = 1;

pub const DEFAULT_PARTITIONS: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Meta {
	pub version: u32,
	pub partitions: u32,
	pub segment_bytes: ByteSize,
	pub index_interval: ByteSize,
	pub segment_age: Duration,
}

impl Meta {
	pub fn encode(&self) -> [u8; META_BYTES] {
		let mut out = [0u8; META_BYTES];
		out[0..4].copy_from_slice(&MAGIC.to_le_bytes());
		out[8..12].copy_from_slice(&self.version.to_le_bytes());
		out[12..16].copy_from_slice(&self.partitions.to_le_bytes());
		out[16..24].copy_from_slice(&self.segment_bytes.as_bytes().to_le_bytes());
		out[24..32].copy_from_slice(&self.index_interval.as_bytes().to_le_bytes());
		out[32..36].copy_from_slice(&self.segment_age.get_months().to_le_bytes());
		out[36..40].copy_from_slice(&self.segment_age.get_days().to_le_bytes());
		out[40..48].copy_from_slice(&self.segment_age.get_nanos().to_le_bytes());
		let checksum = checksum(&out[8..]);
		out[4..8].copy_from_slice(&checksum.to_le_bytes());
		out
	}

	pub fn decode(buf: &[u8; META_BYTES]) -> Option<Self> {
		if u32::from_le_bytes(buf[0..4].try_into().unwrap()) != MAGIC {
			return None;
		}
		if u32::from_le_bytes(buf[4..8].try_into().unwrap()) != checksum(&buf[8..]) {
			return None;
		}
		let segment_age = Duration::new(
			i32::from_le_bytes(buf[32..36].try_into().unwrap()),
			i32::from_le_bytes(buf[36..40].try_into().unwrap()),
			i64::from_le_bytes(buf[40..48].try_into().unwrap()),
		)
		.ok()?;
		Some(Self {
			version: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
			partitions: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
			segment_bytes: ByteSize::from_bytes(u64::from_le_bytes(buf[16..24].try_into().unwrap())),
			index_interval: ByteSize::from_bytes(u64::from_le_bytes(buf[24..32].try_into().unwrap())),
			segment_age,
		})
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

	fn meta() -> Meta {
		Meta {
			version: FORMAT_VERSION,
			partitions: DEFAULT_PARTITIONS,
			segment_bytes: ByteSize::from_mib(256),
			index_interval: ByteSize::from_kib(4),
			segment_age: Duration::from_seconds_const(60),
		}
	}

	#[test]
	fn meta_lays_its_fields_out_at_the_documented_offsets() {
		// The offsets are the on disk format. Meta is the one file that cannot be rebuilt from
		// anything else, so moving a field makes every existing log unopenable.
		let raw = Meta {
			version: 0x01020304,
			partitions: 0x11121314,
			segment_bytes: ByteSize::from_bytes(0x2122232425262728),
			index_interval: ByteSize::from_bytes(0x3132333435363738),
			segment_age: Duration::new(0x41424344, 0x51525354, 0x1122334455).unwrap(),
		}
		.encode();

		assert_eq!(raw.len(), META_BYTES);
		assert_eq!(&raw[0..4], &MAGIC.to_le_bytes());
		assert_eq!(&raw[8..12], &0x01020304u32.to_le_bytes());
		assert_eq!(&raw[12..16], &0x11121314u32.to_le_bytes());
		assert_eq!(&raw[16..24], &0x2122232425262728u64.to_le_bytes());
		assert_eq!(&raw[24..32], &0x3132333435363738u64.to_le_bytes());
		assert_eq!(&raw[32..36], &0x41424344i32.to_le_bytes());
		assert_eq!(&raw[36..40], &0x51525354i32.to_le_bytes());
		assert_eq!(&raw[40..48], &0x1122334455i64.to_le_bytes());
	}

	#[test]
	fn a_meta_round_trips_through_its_bytes() {
		assert_eq!(Meta::decode(&meta().encode()), Some(meta()));
	}

	#[test]
	fn a_duration_with_a_months_component_survives_the_round_trip() {
		// Decision 205: Duration is a months, days and nanos triple, so encoding it as a single
		// nanosecond count would drop the months and silently shorten the age bound.
		let with_months = Meta {
			segment_age: Duration::new(3, 2, 1).unwrap(),
			..meta()
		};

		let back = Meta::decode(&with_months.encode()).unwrap();

		assert_eq!(back.segment_age.get_months(), 3);
		assert_eq!(back.segment_age.get_days(), 2);
		assert_eq!(back.segment_age.get_nanos(), 1);
	}

	#[test]
	fn a_foreign_file_is_refused_by_its_magic() {
		let mut raw = meta().encode();
		raw[0..4].copy_from_slice(&u32::from_le_bytes(*b"RIDX").to_le_bytes());

		assert_eq!(Meta::decode(&raw), None);
	}

	#[test]
	fn a_flipped_bit_in_the_partition_count_is_caught_by_the_checksum() {
		// The partition count decides where every table's records live, so a bit flip here
		// routes reads at a directory that was never written rather than failing to open.
		let mut raw = meta().encode();
		raw[12] ^= 0x01;

		assert_eq!(Meta::decode(&raw), None);
	}

	#[test]
	fn a_flipped_bit_in_the_segment_size_is_caught_by_the_checksum() {
		let mut raw = meta().encode();
		raw[16] ^= 0x01;

		assert_eq!(Meta::decode(&raw), None);
	}

	#[test]
	fn an_unconstructible_duration_is_refused_rather_than_normalised() {
		// Days and nanos must share a sign; a torn write that leaves them opposed must not
		// decode into some other duration that then drives a roll boundary nothing chose.
		let mut raw = meta().encode();
		raw[36..40].copy_from_slice(&1i32.to_le_bytes());
		raw[40..48].copy_from_slice(&(-1i64).to_le_bytes());
		let fixed = checksum(&raw[8..]);
		raw[4..8].copy_from_slice(&fixed.to_le_bytes());

		assert_eq!(Meta::decode(&raw), None);
	}
}
