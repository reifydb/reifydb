// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::identity::IdentityId;

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::identity::RoleId;

#[derive(Debug, Clone, PartialEq)]
pub struct GrantedRoleKey {
	pub identity: IdentityId,
	pub role: RoleId,
}

impl GrantedRoleKey {
	pub fn new(identity: IdentityId, role: RoleId) -> Self {
		Self {
			identity,
			role,
		}
	}

	pub fn encoded(identity: IdentityId, role: RoleId) -> EncodedKey {
		Self::new(identity, role).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(1);
		start.extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(1);
		end.extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn identity_scan(identity: IdentityId) -> EncodedKeyRange {
		let mut prefix = KeySerializer::with_capacity(17);
		prefix.extend_u8(Self::KIND as u8).extend_identity_id(&identity);
		EncodedKeyRange::prefix(prefix.to_encoded_key().as_slice())
	}
}

impl EncodableKey for GrantedRoleKey {
	const KIND: KeyKind = KeyKind::GrantedRole;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer.extend_u8(Self::KIND as u8).extend_identity_id(&self.identity).extend_u64(self.role);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}
		let identity = de.read_identity_id().ok()?;
		let role = de.read_u64().ok()?;
		Some(Self {
			identity,
			role,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use reifydb_value::value::{identity::IdentityId, uuid::Uuid7};
	use uuid::Uuid;

	use super::*;

	fn identity(byte: u8) -> IdentityId {
		IdentityId::from(Uuid7::from(Uuid::from_bytes([byte; 16])))
	}

	#[test]
	fn identity_scan_holds_every_role_of_that_identity() {
		// Small ids complement to a suffix starting 0xFF, so a prefix+0xFF end bound excludes them all.
		let alice = identity(1);
		let range = GrantedRoleKey::identity_scan(alice);

		for role in [0u64, 1, 2, u64::MAX] {
			let key = GrantedRoleKey::encoded(alice, role);
			assert!(range.contains(&key), "role {role} must fall inside the identity scan");
		}
	}

	#[test]
	fn identity_scan_excludes_a_neighbouring_identity() {
		// The scan must not widen into the next identity when the bound is carry-incremented.
		let range = GrantedRoleKey::identity_scan(identity(1));

		assert!(!range.contains(&GrantedRoleKey::encoded(identity(2), 1)));
	}
}
