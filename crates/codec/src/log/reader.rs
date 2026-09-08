// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crc32fast::Hasher;

use crate::log::LogVersion;

pub const HINT_BYTES: usize = 16;

pub const MAGIC: u32 = u32::from_le_bytes(*b"RRDR");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hint {
	pub version: LogVersion,
}

impl Hint {
	pub fn new(version: LogVersion) -> Self {
		Self {
			version,
		}
	}

	pub fn encode(&self) -> [u8; HINT_BYTES] {
		let mut out = [0u8; HINT_BYTES];
		out[0..4].copy_from_slice(&MAGIC.to_le_bytes());
		out[8..16].copy_from_slice(&self.version.as_u64().to_le_bytes());
		let checksum = checksum(&out[8..]);
		out[4..8].copy_from_slice(&checksum.to_le_bytes());
		out
	}

	pub fn decode(buf: &[u8; HINT_BYTES]) -> Option<Self> {
		if u32::from_le_bytes(buf[0..4].try_into().unwrap()) != MAGIC {
			return None;
		}
		if u32::from_le_bytes(buf[4..8].try_into().unwrap()) != checksum(&buf[8..]) {
			return None;
		}
		Some(Self {
			version: LogVersion::new(u64::from_le_bytes(buf[8..16].try_into().unwrap())),
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

	#[test]
	fn a_hint_lays_its_fields_out_at_the_documented_offsets() {
		// the hint is read by a purge that is about to unlink files, so a field that moves makes an
		// old hint decode as some other version and the log deletes a segment a reader still needs.
		let raw = Hint::new(LogVersion::new(0x0102030405060708)).encode();

		assert_eq!(raw.len(), HINT_BYTES);
		assert_eq!(&raw[0..4], &MAGIC.to_le_bytes());
		assert_eq!(&raw[4..8], &0xa5cced25u32.to_le_bytes());
		assert_eq!(&raw[8..16], &0x0102030405060708u64.to_le_bytes());
	}

	#[test]
	fn a_hint_round_trips_through_its_bytes() {
		let hint = Hint::new(LogVersion::new(4096));

		assert_eq!(Hint::decode(&hint.encode()), Some(hint));
	}

	#[test]
	fn a_hint_of_all_zeros_does_not_decode() {
		// decision 234: the hint is published without an fsync, so a crash can make the name durable
		// before the bytes. zeros must read as unreadable, which pins at zero, never as version zero
		// arrived at legitimately, which would be indistinguishable from a reader that has read nothing.
		assert_eq!(Hint::decode(&[0u8; HINT_BYTES]), None);
	}

	#[test]
	fn a_flipped_bit_anywhere_in_the_version_is_caught_by_the_checksum() {
		// decision 239: retention unlinks segments on this number, and half the bit flips in the
		// version word raise it. A raised floor deletes records a reader still needs, silently and for
		// good, so every flip has to fail the decode and fall back to pinning at the beginning.
		for at in 8..HINT_BYTES {
			let mut raw = Hint::new(LogVersion::new(500_000)).encode();
			raw[at] ^= 0x01;

			assert_eq!(Hint::decode(&raw), None, "a flip at byte {at} decoded anyway");
		}
	}

	#[test]
	fn a_flipped_bit_in_the_reserved_word_is_caught_too() {
		// the checksum lives there, so a file damaged in the one place that used to be ignored now
		// reads as damaged rather than as a hint nobody wrote.
		for at in 4..8 {
			let mut raw = Hint::new(LogVersion::new(500_000)).encode();
			raw[at] ^= 0x01;

			assert_eq!(Hint::decode(&raw), None, "a flip at byte {at} decoded anyway");
		}
	}

	#[test]
	fn a_foreign_file_is_refused_by_its_magic() {
		let mut raw = Hint::new(LogVersion::new(7)).encode();
		raw[0..4].copy_from_slice(&u32::from_le_bytes(*b"RIDX").to_le_bytes());

		assert_eq!(Hint::decode(&raw), None);
	}
}
