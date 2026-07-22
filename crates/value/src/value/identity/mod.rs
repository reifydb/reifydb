// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt, ops::Deref, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::Visitor};
use uuid::Uuid;

use crate::{
	clock::{ClockNow, RandomBytes},
	value::uuid::Uuid7,
};

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Default)]
pub struct IdentityId(pub Uuid7);

impl IdentityId {
	pub fn generate<C: ClockNow, R: RandomBytes>(clock: &C, rng: &R) -> Self {
		IdentityId(Uuid7::generate(clock, rng))
	}

	pub fn new(id: Uuid7) -> Self {
		IdentityId(id)
	}

	pub fn value(&self) -> Uuid7 {
		self.0
	}

	pub fn anonymous() -> Self {
		let bytes = [
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];
		IdentityId(Uuid7(Uuid::from_bytes(bytes)))
	}

	pub fn root() -> Self {
		let bytes = [
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0xFF, 0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		];
		IdentityId(Uuid7(Uuid::from_bytes(bytes)))
	}

	pub fn system() -> Self {
		let bytes = [
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0x7F, 0xFF, 0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		];
		IdentityId(Uuid7(Uuid::from_bytes(bytes)))
	}

	pub fn is_anonymous(&self) -> bool {
		*self == Self::anonymous()
	}

	pub fn is_root(&self) -> bool {
		*self == Self::root()
	}

	pub fn is_system(&self) -> bool {
		*self == Self::system()
	}

	pub fn sentinel_kind(&self) -> Option<IdentityKind> {
		if self.is_root() {
			Some(IdentityKind::Root)
		} else if self.is_system() {
			Some(IdentityKind::System)
		} else if self.is_anonymous() {
			Some(IdentityKind::Anonymous)
		} else {
			None
		}
	}

	pub fn is_privileged(&self) -> bool {
		matches!(self.sentinel_kind(), Some(IdentityKind::Root | IdentityKind::System))
	}
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IdentityKind {
	User = 0,
	Service = 1,
	Root = 2,
	System = 3,
	Anonymous = 4,
}

impl IdentityKind {
	pub fn to_u8(self) -> u8 {
		match self {
			IdentityKind::User => 0,
			IdentityKind::Service => 1,
			IdentityKind::Root => 2,
			IdentityKind::System => 3,
			IdentityKind::Anonymous => 4,
		}
	}

	pub fn from_u8(value: u8) -> Self {
		match value {
			0 => IdentityKind::User,
			1 => IdentityKind::Service,
			2 => IdentityKind::Root,
			3 => IdentityKind::System,
			4 => IdentityKind::Anonymous,
			_ => IdentityKind::User,
		}
	}

	pub fn as_str(self) -> &'static str {
		match self {
			IdentityKind::User => "user",
			IdentityKind::Service => "service",
			IdentityKind::Root => "root",
			IdentityKind::System => "system",
			IdentityKind::Anonymous => "anonymous",
		}
	}

	pub fn is_builtin(self) -> bool {
		matches!(self, IdentityKind::Root | IdentityKind::System | IdentityKind::Anonymous)
	}
}

impl fmt::Display for IdentityKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.as_str())
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
		Serialize::serialize(&self.0, serializer)
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
				E: de::Error,
			{
				let uuid =
					Uuid::from_str(value).map_err(|e| E::custom(format!("invalid UUID: {}", e)))?;

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
				E: de::Error,
			{
				let uuid = Uuid::from_slice(value)
					.map_err(|e| E::custom(format!("invalid UUID bytes: {}", e)))?;

				if uuid.get_version_num() != 7 {
					return Err(E::custom(format!(
						"expected UUID v7, got v{}",
						uuid.get_version_num()
					)));
				}

				Ok(IdentityId(Uuid7::from(uuid)))
			}
		}

		if deserializer.is_human_readable() {
			deserializer.deserialize_str(Uuid7Visitor)
		} else {
			deserializer.deserialize_bytes(Uuid7Visitor)
		}
	}
}

#[cfg(test)]
pub mod tests {
	use postcard::{from_bytes, to_allocvec};
	use serde_json::{from_str, to_string};

	use super::*;
	use crate::clock::testing::{TestClock, TestRng};

	fn test_clock_and_rng() -> (TestClock, TestClock, TestRng) {
		let clock = TestClock::from_millis(1000);
		(clock.clone(), clock, TestRng)
	}

	#[test]
	fn test_identity_id_creation() {
		let (_, clock, rng) = test_clock_and_rng();
		let id = IdentityId::generate(&clock, &rng);
		assert_ne!(id, IdentityId::default());
	}

	#[test]
	fn test_identity_id_from_uuid7() {
		let (_, clock, rng) = test_clock_and_rng();
		let uuid = Uuid7::generate(&clock, &rng);
		let id = IdentityId::from(uuid);
		assert_eq!(id.value(), uuid);
	}

	#[test]
	fn test_identity_id_display() {
		let (_, clock, rng) = test_clock_and_rng();
		let id = IdentityId::generate(&clock, &rng);
		let display = format!("{}", id);
		assert!(!display.is_empty());
	}

	#[test]
	fn test_identity_id_equality() {
		let (_, clock, rng) = test_clock_and_rng();
		let uuid = Uuid7::generate(&clock, &rng);
		let id1 = IdentityId::from(uuid);
		let id2 = IdentityId::from(uuid);
		assert_eq!(id1, id2);
	}

	#[test]
	fn test_identity_id_postcard_roundtrip() {
		let (_, clock, rng) = test_clock_and_rng();
		let id = IdentityId::generate(&clock, &rng);
		let bytes = to_allocvec(&id).expect("postcard serialize");
		let decoded: IdentityId = from_bytes(&bytes).expect("postcard deserialize");
		assert_eq!(id, decoded);
	}

	#[test]
	fn test_identity_id_postcard_roundtrip_root() {
		let id = IdentityId::root();
		let bytes = to_allocvec(&id).expect("postcard serialize root");
		let decoded: IdentityId = from_bytes(&bytes).expect("postcard deserialize root");
		assert_eq!(id, decoded);
	}

	#[test]
	fn test_identity_id_json_roundtrip() {
		let (_, clock, rng) = test_clock_and_rng();
		let id = IdentityId::generate(&clock, &rng);
		let s = to_string(&id).expect("json serialize");
		let decoded: IdentityId = from_str(&s).expect("json deserialize");
		assert_eq!(id, decoded);
	}

	#[test]
	fn test_sentinel_kind_covers_all_three_sentinels() {
		// The sentinels have no catalog row, so their kind can only come from
		// the id itself. A None here would make the resolution rule
		// sentinel_kind().unwrap_or(stored) fall through to a stored kind that
		// does not exist.
		assert_eq!(IdentityId::root().sentinel_kind(), Some(IdentityKind::Root));
		assert_eq!(IdentityId::system().sentinel_kind(), Some(IdentityKind::System));
		assert_eq!(IdentityId::anonymous().sentinel_kind(), Some(IdentityKind::Anonymous));
	}

	#[test]
	fn test_sentinel_kind_is_none_for_a_regular_identity() {
		// A generated id must defer to its stored kind, otherwise every
		// identity would be forced into a builtin kind.
		let (_, clock, rng) = test_clock_and_rng();
		assert_eq!(IdentityId::generate(&clock, &rng).sentinel_kind(), None);
	}

	#[test]
	fn test_default_identity_id_is_not_anonymous() {
		// IdentityId derives Default (all-zero Uuid7), which is a distinct
		// value from the anonymous sentinel (that one carries version and
		// variant bits). Conflating them would hand a default id the
		// anonymous kind.
		assert_ne!(IdentityId::default(), IdentityId::anonymous());
		assert_eq!(IdentityId::default().sentinel_kind(), None);
	}

	#[test]
	fn test_is_privileged_is_root_and_system_only() {
		// is_privileged gates all five policy bypass sites. Anonymous must
		// never be privileged.
		assert!(IdentityId::root().is_privileged());
		assert!(IdentityId::system().is_privileged());
		assert!(!IdentityId::anonymous().is_privileged());
		let (_, clock, rng) = test_clock_and_rng();
		assert!(!IdentityId::generate(&clock, &rng).is_privileged());
	}

	#[test]
	fn test_identity_kind_u8_roundtrip() {
		// The u8 is the on-disk representation; a mismatch silently
		// reinterprets stored identities as a different kind.
		for kind in [
			IdentityKind::User,
			IdentityKind::Service,
			IdentityKind::Root,
			IdentityKind::System,
			IdentityKind::Anonymous,
		] {
			assert_eq!(IdentityKind::from_u8(kind.to_u8()), kind);
		}
	}

	#[test]
	fn test_identity_kind_user_is_zero() {
		// User must be 0 so that a row written before the kind field existed
		// decodes from zeroed padding as User rather than as a builtin kind.
		assert_eq!(IdentityKind::User.to_u8(), 0);
	}

	#[test]
	fn test_identity_kind_from_unknown_u8_falls_back_to_user() {
		// from_u8 is total by house convention (see FlowStatus). An unknown
		// byte must not panic, and must not decode as a builtin kind, which
		// would grant it the DROP/ALTER/GRANT immunity builtins get.
		let kind = IdentityKind::from_u8(200);
		assert_eq!(kind, IdentityKind::User);
		assert!(!kind.is_builtin());
	}

	#[test]
	fn test_is_builtin_matches_the_unstorable_kinds() {
		// is_builtin gates DROP/ALTER/GRANT. It must cover exactly the kinds
		// that are never stored, so User and Service stay reachable by DDL.
		assert!(IdentityKind::Root.is_builtin());
		assert!(IdentityKind::System.is_builtin());
		assert!(IdentityKind::Anonymous.is_builtin());
		assert!(!IdentityKind::User.is_builtin());
		assert!(!IdentityKind::Service.is_builtin());
	}
}
