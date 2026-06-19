// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionEpochKey {
	pub bucket_nanos: u64,
}

impl EncodableKey for VersionEpochKey {
	const KIND: KeyKind = KeyKind::VersionEpoch;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.bucket_nanos);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let bucket_nanos = de.read_u64().ok()?;

		Some(Self {
			bucket_nanos,
		})
	}
}

impl VersionEpochKey {
	pub fn encoded(bucket_nanos: u64) -> EncodedKey {
		Self {
			bucket_nanos,
		}
		.encode()
	}

	pub fn floor_scan(target_nanos: u64) -> EncodedKeyRange {
		EncodedKeyRange::new(Bound::Included(Self::encoded(target_nanos)), Bound::Included(Self::encoded(0)))
	}

	pub fn older_than(cutoff_nanos: u64) -> EncodedKeyRange {
		EncodedKeyRange::new(Bound::Excluded(Self::encoded(cutoff_nanos)), Bound::Included(Self::encoded(0)))
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::{EncodableKey, VersionEpochKey};

	#[test]
	fn test_encode_decode() {
		let key = VersionEpochKey {
			bucket_nanos: 0x0123456789ABCDEF,
		};
		let encoded = key.encode();
		let decoded = VersionEpochKey::decode(&encoded).unwrap();
		assert_eq!(decoded.bucket_nanos, 0x0123456789ABCDEF);
	}

	#[test]
	fn test_descending_order_so_newer_bucket_sorts_first() {
		let older = VersionEpochKey::encoded(100);
		let newer = VersionEpochKey::encoded(200);
		assert!(
			newer < older,
			"a newer (larger) bucket must encode to smaller key bytes so floor_scan can take the first entry at-or-after the target"
		);
	}

	#[test]
	fn test_floor_scan_lower_bound_is_target_bucket() {
		let target = 150u64;
		let range = VersionEpochKey::floor_scan(target);
		assert_eq!(range.start, Bound::Included(VersionEpochKey::encoded(target)));
		assert_eq!(range.end, Bound::Included(VersionEpochKey::encoded(0)));
		// A bucket exactly at the target is included; a bucket newer than the target is excluded.
		assert!(VersionEpochKey::encoded(target) >= VersionEpochKey::encoded(target));
		assert!(VersionEpochKey::encoded(target + 1) < VersionEpochKey::encoded(target));
	}
}
