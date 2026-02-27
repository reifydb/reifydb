// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{fmt, ops::Deref, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

use crate::value::uuid::Uuid7;

/// An identity identifier - a unique UUID v7 for an identity
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Default)]
pub struct IdentityId(pub Uuid7);

impl IdentityId {
	/// Create a new IdentityId with a generated UUID v7
	pub fn generate() -> Self {
		IdentityId(Uuid7::generate())
	}

	/// Create a new IdentityId from an existing Uuid7
	pub fn new(id: Uuid7) -> Self {
		IdentityId(id)
	}

	/// Get the inner Uuid7 value
	pub fn value(&self) -> Uuid7 {
		self.0
	}

	/// Sentinel for anonymous identity: minimum valid UUID v7
	/// `00000000-0000-7000-8000-000000000000`
	pub fn anonymous() -> Self {
		let bytes = [
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];
		IdentityId(Uuid7(uuid::Uuid::from_bytes(bytes)))
	}

	/// Sentinel for root/system identity: maximum valid UUID v7
	/// `ffffffff-ffff-7fff-bfff-ffffffffffff`
	pub fn root() -> Self {
		let bytes = [
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0xFF, 0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		];
		IdentityId(Uuid7(uuid::Uuid::from_bytes(bytes)))
	}

	pub fn is_anonymous(&self) -> bool {
		*self == Self::anonymous()
	}

	pub fn is_root(&self) -> bool {
		*self == Self::root()
	}
}

impl Deref for IdentityId {
	type Target = Uuid7;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<Uuid7> for IdentityId {
	fn eq(&self, other: &Uuid7) -> bool {
		self.0.eq(other)
	}
}

impl From<Uuid7> for IdentityId {
	fn from(id: Uuid7) -> Self {
		IdentityId(id)
	}
}

impl From<IdentityId> for Uuid7 {
	fn from(identity_id: IdentityId) -> Self {
		identity_id.0
	}
}

impl fmt::Display for IdentityId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl Serialize for IdentityId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.0.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for IdentityId {
	fn deserialize<D>(deserializer: D) -> Result<IdentityId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct Uuid7Visitor;

		impl<'de> Visitor<'de> for Uuid7Visitor {
			type Value = IdentityId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a UUID version 7")
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				let uuid = uuid::Uuid::from_str(value)
					.map_err(|e| E::custom(format!("invalid UUID: {}", e)))?;

				if uuid.get_version_num() != 7 {
					return Err(E::custom(format!(
						"expected UUID v7, got v{}",
						uuid.get_version_num()
					)));
				}

				Ok(IdentityId(Uuid7::from(uuid)))
			}

			fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				let uuid = uuid::Uuid::from_slice(value)
					.map_err(|e| E::custom(format!("invalid UUID bytes: {}", e)))?;

				// Verify it's a v7 UUID or nil
				if uuid.get_version_num() != 7 {
					return Err(E::custom(format!(
						"expected UUID v7, got v{}",
						uuid.get_version_num()
					)));
				}

				Ok(IdentityId(Uuid7::from(uuid)))
			}
		}

		deserializer.deserialize_any(Uuid7Visitor)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_identity_id_creation() {
		let id = IdentityId::generate();
		assert_ne!(id, IdentityId::default());
	}

	#[test]
	fn test_identity_id_from_uuid7() {
		let uuid = Uuid7::generate();
		let id = IdentityId::from(uuid);
		assert_eq!(id.value(), uuid);
	}

	#[test]
	fn test_identity_id_display() {
		let id = IdentityId::generate();
		let display = format!("{}", id);
		assert!(!display.is_empty());
	}

	#[test]
	fn test_identity_id_equality() {
		let uuid = Uuid7::generate();
		let id1 = IdentityId::from(uuid);
		let id2 = IdentityId::from(uuid);
		assert_eq!(id1, id2);
	}
}
