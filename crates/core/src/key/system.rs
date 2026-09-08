// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;
use reifydb_runtime::version_epoch::EpochSeconds;
use serde::{Deserialize, Serialize, de};

use super::KeyTag;
use crate::{
	interface::catalog::id::{MigrationEventId, MigrationId, SequenceId},
	key::{
		any::{Field, KeyFields, Width},
		bound::{TaggedKeyBound, TaggedKeyBoundRange},
	},
};

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = SystemSequence)]
pub struct SystemSequenceKey {
	pub sequence: SequenceId,
}

impl SystemSequenceKey {
	pub fn new(sequence: impl Into<SequenceId>) -> Self {
		Self {
			sequence: sequence.into(),
		}
	}

	pub fn encoded(sequence: impl Into<SequenceId>) -> EncodedKey {
		Self {
			sequence: sequence.into(),
		}
		.encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod system_sequence_key_tests {
	use super::SystemSequenceKey;
	use crate::interface::catalog::id::SequenceId;

	#[test]
	fn test_encode_decode() {
		let key = SystemSequenceKey {
			sequence: SequenceId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xFA, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = SystemSequenceKey::decode(&encoded).unwrap();
		assert_eq!(key.sequence.0, 0xABCD);
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = SystemVersion)]
pub struct SystemVersionKey {
	#[key(repr = u8)]
	pub version: SystemVersion,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum SystemVersion {
	Storage = 0x01,
}

impl From<SystemVersion> for u8 {
	fn from(version: SystemVersion) -> Self {
		version as u8
	}
}
impl TryFrom<u8> for SystemVersion {
	type Error = de::value::Error;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0x01 => Ok(Self::Storage),
			_ => Err(de::Error::custom(format!("Invalid SystemVersion value: {value:#04x}"))),
		}
	}
}

impl SystemVersionKey {
	pub fn new(version: SystemVersion) -> Self {
		Self {
			version,
		}
	}

	pub fn encoded(version: SystemVersion) -> EncodedKey {
		Self {
			version,
		}
		.encode()
	}
}

#[cfg(test)]
pub mod system_version_key_tests {
	use super::{SystemVersion, SystemVersionKey};

	#[test]
	fn test_encode_decode_storage_version() {
		let key = SystemVersionKey {
			version: SystemVersion::Storage,
		};
		let encoded = key.encode();
		let expected = vec![0xF5, 0xFE];
		assert_eq!(encoded.as_slice(), expected);

		let key = SystemVersionKey::decode(&encoded).unwrap();
		assert_eq!(key.version, SystemVersion::Storage);
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = TransactionVersion)]
pub struct TransactionVersionKey {}

impl TransactionVersionKey {
	pub fn encoded() -> EncodedKey {
		Self {}.encode()
	}
}

#[cfg(test)]
pub mod transaction_version_key_tests {
	use super::TransactionVersionKey;

	#[test]
	fn test_encode_decode() {
		let key = TransactionVersionKey {};
		let encoded = key.encode();
		let expected = vec![0xF4];
		assert_eq!(encoded.as_slice(), expected);

		TransactionVersionKey::decode(&encoded).unwrap();
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, KeyCodec, Hash)]
#[key(tag = VersionEpoch)]
pub struct VersionEpochKey {
	pub bucket: EpochSeconds,
}

impl VersionEpochKey {
	pub fn new(bucket: EpochSeconds) -> Self {
		Self {
			bucket,
		}
	}

	pub fn encoded(bucket: EpochSeconds) -> EncodedKey {
		Self::new(bucket).encode()
	}

	fn bucket_bound(bucket: EpochSeconds) -> TaggedKeyBound {
		TaggedKeyBound::prefix(Self::TAG, [Field::UDesc(Width::U64, bucket.seconds() as u128)])
	}

	pub fn floor_scan(target: EpochSeconds) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange {
			start: Bound::Included(Self::bucket_bound(target)),
			end: Bound::Included(Self::bucket_bound(EpochSeconds::new(0))),
		}
	}

	pub fn older_than(cutoff: EpochSeconds) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange {
			start: Bound::Excluded(Self::bucket_bound(cutoff)),
			end: Bound::Included(Self::bucket_bound(EpochSeconds::new(0))),
		}
	}
}

#[cfg(test)]
mod version_epoch_key_tests {
	use std::ops::Bound;

	use super::{EpochSeconds, VersionEpochKey};

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
		let range = VersionEpochKey::floor_scan(target).encode();
		assert_eq!(range.start, Bound::Included(VersionEpochKey::encoded(target)));
		assert_eq!(range.end, Bound::Included(VersionEpochKey::encoded(sec(0))));
		// A bucket exactly at the target is included; a bucket newer than the target is excluded.
		assert!(VersionEpochKey::encoded(target) >= VersionEpochKey::encoded(target));
		assert!(VersionEpochKey::encoded(sec(151)) < VersionEpochKey::encoded(target));
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Migration)]
pub struct MigrationKey {
	pub migration: MigrationId,
}

impl MigrationKey {
	pub fn new(migration: MigrationId) -> Self {
		Self {
			migration,
		}
	}

	pub fn encoded(migration: impl Into<MigrationId>) -> EncodedKey {
		Self::new(migration.into()).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod migration_key_tests {
	use super::MigrationKey;
	use crate::interface::catalog::id::MigrationId;

	#[test]
	fn test_encode_decode() {
		let key = MigrationKey {
			migration: MigrationId(0xABCD),
		};
		let encoded = key.encode();
		let decoded = MigrationKey::decode(&encoded).unwrap();
		assert_eq!(decoded.migration, MigrationId(0xABCD));
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = MigrationEvent)]
pub struct MigrationEventKey {
	pub event: MigrationEventId,
}

impl MigrationEventKey {
	pub fn new(event: MigrationEventId) -> Self {
		Self {
			event,
		}
	}

	pub fn encoded(event: impl Into<MigrationEventId>) -> EncodedKey {
		Self::new(event.into()).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod migration_event_key_tests {
	use super::MigrationEventKey;
	use crate::interface::catalog::id::MigrationEventId;

	#[test]
	fn test_encode_decode() {
		let key = MigrationEventKey {
			event: MigrationEventId(0xABCD),
		};
		let encoded = key.encode();
		let decoded = MigrationEventKey::decode(&encoded).unwrap();
		assert_eq!(decoded.event, MigrationEventId(0xABCD));
	}
}
