// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{self, Display, Formatter},
	ops::Add,
};

use serde::{Deserialize, Serialize};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ByteSize(u64);

impl ByteSize {
	pub const ZERO: Self = Self(0);

	pub const fn from_bytes(bytes: u64) -> Self {
		Self(bytes)
	}

	pub const fn from_kib(kib: u64) -> Self {
		Self(kib * KIB)
	}

	pub const fn from_mib(mib: u64) -> Self {
		Self(mib * MIB)
	}

	pub const fn from_gib(gib: u64) -> Self {
		Self(gib * GIB)
	}

	pub const fn as_bytes(self) -> u64 {
		self.0
	}

	pub const fn as_kib(self) -> u64 {
		self.0 / KIB
	}

	pub const fn saturating_add(self, other: Self) -> Self {
		Self(self.0.saturating_add(other.0))
	}

	pub const fn saturating_sub(self, other: Self) -> Self {
		Self(self.0.saturating_sub(other.0))
	}
}

impl Add for ByteSize {
	type Output = Self;

	fn add(self, rhs: Self) -> Self {
		Self(self.0 + rhs.0)
	}
}

impl Display for ByteSize {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let bytes = self.0;
		if bytes >= GIB && bytes.is_multiple_of(GIB) {
			write!(f, "{} GiB", bytes / GIB)
		} else if bytes >= MIB && bytes.is_multiple_of(MIB) {
			write!(f, "{} MiB", bytes / MIB)
		} else if bytes >= KIB && bytes.is_multiple_of(KIB) {
			write!(f, "{} KiB", bytes / KIB)
		} else {
			write!(f, "{} B", bytes)
		}
	}
}

impl From<ByteSize> for u64 {
	fn from(size: ByteSize) -> Self {
		size.0
	}
}

impl From<u64> for ByteSize {
	fn from(bytes: u64) -> Self {
		Self(bytes)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_from_bytes_round_trips() {
		assert_eq!(ByteSize::from_bytes(4096).as_bytes(), 4096);
	}

	#[test]
	fn test_unit_constructors_use_binary_base() {
		assert_eq!(ByteSize::from_kib(1).as_bytes(), 1024);
		assert_eq!(ByteSize::from_mib(64).as_bytes(), 64 * 1024 * 1024);
		assert_eq!(ByteSize::from_gib(1).as_bytes(), 1024 * 1024 * 1024);
	}

	#[test]
	fn test_as_kib_matches_sqlite_cache_semantics() {
		// SQLite negative cache_size is interpreted in KiB; from_kib(2000) must read back as 2000.
		assert_eq!(ByteSize::from_kib(2000).as_kib(), 2000);
	}

	#[test]
	fn test_as_kib_truncates_sub_kib_remainder() {
		assert_eq!(ByteSize::from_bytes(2047).as_kib(), 1);
		assert_eq!(ByteSize::from_bytes(1023).as_kib(), 0);
	}

	#[test]
	fn test_zero() {
		assert_eq!(ByteSize::ZERO.as_bytes(), 0);
		assert_eq!(ByteSize::ZERO, ByteSize::from_bytes(0));
	}

	#[test]
	fn test_ordering_compares_by_byte_count() {
		assert!(ByteSize::from_kib(1) < ByteSize::from_mib(1));
		assert!(ByteSize::from_mib(256) > ByteSize::from_mib(64));
		assert_eq!(ByteSize::from_kib(1024), ByteSize::from_mib(1));
	}

	#[test]
	fn test_display_picks_largest_exact_unit() {
		assert_eq!(ByteSize::from_gib(2).to_string(), "2 GiB");
		assert_eq!(ByteSize::from_mib(64).to_string(), "64 MiB");
		assert_eq!(ByteSize::from_kib(2000).to_string(), "2000 KiB");
		assert_eq!(ByteSize::from_bytes(4096).to_string(), "4 KiB");
		assert_eq!(ByteSize::from_bytes(1500).to_string(), "1500 B");
		assert_eq!(ByteSize::ZERO.to_string(), "0 B");
	}

	#[test]
	fn test_add_sums_bytes() {
		assert_eq!(ByteSize::from_kib(1) + ByteSize::from_kib(3), ByteSize::from_kib(4));
		assert_eq!(ByteSize::from_mib(1) + ByteSize::from_bytes(512), ByteSize::from_bytes(1024 * 1024 + 512));
		assert_eq!(ByteSize::ZERO + ByteSize::from_bytes(7), ByteSize::from_bytes(7));
	}

	#[test]
	fn test_saturating_add_clamps_at_max() {
		assert_eq!(ByteSize::from_bytes(10).saturating_add(ByteSize::from_bytes(5)), ByteSize::from_bytes(15));
		assert_eq!(
			ByteSize::from_bytes(u64::MAX).saturating_add(ByteSize::from_bytes(1)),
			ByteSize::from_bytes(u64::MAX)
		);
	}

	#[test]
	fn test_saturating_sub_clamps_at_zero() {
		assert_eq!(ByteSize::from_bytes(10).saturating_sub(ByteSize::from_bytes(4)), ByteSize::from_bytes(6));
		assert_eq!(ByteSize::from_bytes(3).saturating_sub(ByteSize::from_bytes(9)), ByteSize::ZERO);
	}
}
