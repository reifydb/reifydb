// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;
use reifydb_runtime::version_epoch::EpochSeconds;
use serde::{Deserialize, Serialize, de};

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::id::{MigrationEventId, MigrationId, SequenceId},
	key::typed::key::Key,
};

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = SystemSequence)]
pub struct SystemSequenceKey {
	pub sequence: SequenceId,
}

impl SystemSequenceKey {
	pub fn encoded(sequence: impl Into<SequenceId>) -> EncodedKey {
		Key::encode(&Self {
			sequence: sequence.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::sequence_start()), Some(Self::sequence_end()))
	}

	fn sequence_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<SystemSequenceKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn sequence_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<SystemSequenceKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod system_sequence_key_tests {
	use super::{Key, SystemSequenceKey};
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

#[derive(Debug, Clone, PartialEq)]
pub struct SystemVersionKey {
	pub version: SystemVersion,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
	pub fn encoded(version: SystemVersion) -> EncodedKey {
		Self {
			version,
		}
		.encode()
	}
}

impl EncodableKey for SystemVersionKey {
	const KIND: KeyKind = KeyKind::SystemVersion;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(2);
		serializer.extend_u8(Self::KIND as u8).extend_u8(self.version as u8);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self>
	where
		Self: Sized,
	{
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let version_enum = de.read_u8().ok()?.try_into().ok()?;

		Some(Self {
			version: version_enum,
		})
	}
}

#[cfg(test)]
pub mod system_version_key_tests {
	use super::{EncodableKey, SystemVersion, SystemVersionKey};

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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = TransactionVersion)]
pub struct TransactionVersionKey {}

impl TransactionVersionKey {
	pub fn encoded() -> EncodedKey {
		Key::encode(&Self {})
	}
}

#[cfg(test)]
pub mod transaction_version_key_tests {
	use super::{Key, TransactionVersionKey};

	#[test]
	fn test_encode_decode() {
		let key = TransactionVersionKey {};
		let encoded = key.encode();
		let expected = vec![0xF4];
		assert_eq!(encoded.as_slice(), expected);

		TransactionVersionKey::decode(&encoded).unwrap();
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Key)]
#[key(kind = VersionEpoch)]
pub struct VersionEpochKey {
	pub bucket: EpochSeconds,
}

impl VersionEpochKey {
	pub fn encoded(bucket: EpochSeconds) -> EncodedKey {
		Key::encode(&Self {
			bucket,
		})
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
mod version_epoch_key_tests {
	use std::ops::Bound;

	use super::{EpochSeconds, Key, VersionEpochKey};

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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Migration)]
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
		Key::encode(&Self::new(migration.into()))
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<MigrationKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<MigrationKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod migration_key_tests {
	use super::{Key, MigrationKey};
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = MigrationEvent)]
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
		Key::encode(&Self::new(event.into()))
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<MigrationEventKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<MigrationEventKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod migration_event_key_tests {
	use super::{Key, MigrationEventKey};
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
