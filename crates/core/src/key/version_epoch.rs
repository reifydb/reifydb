// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_runtime::version_epoch::EpochSeconds;

use super::{EncodableKey, KeyKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionEpochKey {
	pub bucket: EpochSeconds,
}

impl EncodableKey for VersionEpochKey {
	const KIND: KeyKind = KeyKind::VersionEpoch;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.bucket.seconds());
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let bucket = EpochSeconds::new(de.read_u64().ok()?);

		Some(Self {
			bucket,
		})
	}
}

impl VersionEpochKey {
	pub fn encoded(bucket: EpochSeconds) -> EncodedKey {
		Self {
			bucket,
		}
		.encode()
	}

	pub fn floor_scan(target: EpochSeconds) -> EncodedKeyRange {
		EncodedKeyRange::new(
			Bound::Included(Self::encoded(target)),
			Bound::Included(Self::encoded(EpochSeconds::new(0))),
		)
	}

	pub fn older_than(cutoff: EpochSeconds) -> EncodedKeyRange {
		EncodedKeyRange::new(
			Bound::Excluded(Self::encoded(cutoff)),
			Bound::Included(Self::encoded(EpochSeconds::new(0))),
		)
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::{EncodableKey, EpochSeconds, VersionEpochKey};

	fn sec(seconds: u64) -> EpochSeconds {
		EpochSeconds::new(seconds)
	}

	#[test]
	fn test_encode_decode() {
		let key = VersionEpochKey {
			bucket: sec(0x0123456789ABCDEF),
		};
		let encoded = key.encode();
		let decoded = VersionEpochKey::decode(&encoded).unwrap();
		assert_eq!(decoded.bucket, sec(0x0123456789ABCDEF));
	}

	#[test]
	fn test_descending_order_so_newer_bucket_sorts_first() {
		let older = VersionEpochKey::encoded(sec(100));
		let newer = VersionEpochKey::encoded(sec(200));
		assert!(
			newer < older,
			"a newer (larger) bucket must encode to smaller key bytes so floor_scan can take the first entry at-or-after the target"
		);
	}

	#[test]
	fn test_floor_scan_lower_bound_is_target_bucket() {
		let target = sec(150);
		let range = VersionEpochKey::floor_scan(target);
		assert_eq!(range.start, Bound::Included(VersionEpochKey::encoded(target)));
		assert_eq!(range.end, Bound::Included(VersionEpochKey::encoded(sec(0))));
		// A bucket exactly at the target is included; a bucket newer than the target is excluded.
		assert!(VersionEpochKey::encoded(target) >= VersionEpochKey::encoded(target));
		assert!(VersionEpochKey::encoded(sec(151)) < VersionEpochKey::encoded(target));
	}
}
