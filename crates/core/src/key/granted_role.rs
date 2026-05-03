// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::identity::IdentityId;

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::identity::RoleId,
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

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
		let mut start = KeySerializer::with_capacity(2);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(2);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn identity_scan(identity: IdentityId) -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(18);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_identity_id(&identity);
		let mut end = KeySerializer::with_capacity(18);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_identity_id(&identity);

		let start_key = start.to_encoded_key();
		let mut end_bytes = end.to_encoded_key().to_vec();
		end_bytes.push(0xFF);
		EncodedKeyRange::start_end(Some(start_key), Some(EncodedKey::new(end_bytes)))
	}
}

impl EncodableKey for GrantedRoleKey {
	const KIND: KeyKind = KeyKind::GrantedRole;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(VERSION)
			.extend_u8(Self::KIND as u8)
			.extend_identity_id(&self.identity)
			.extend_u64(self.role);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let version = de.read_u8().ok()?;
		if version != VERSION {
			return None;
		}
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
