// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{EncodableKey, KeyKind};
use crate::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::user::{RoleId, UserId},
	util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
};

const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct UserRoleKey {
	pub user: UserId,
	pub role: RoleId,
}

impl UserRoleKey {
	pub fn new(user: UserId, role: RoleId) -> Self {
		Self {
			user,
			role,
		}
	}

	pub fn encoded(user: UserId, role: RoleId) -> EncodedKey {
		Self::new(user, role).encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(2);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8);
		let mut end = KeySerializer::with_capacity(2);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8 - 1);
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn user_scan(user: UserId) -> EncodedKeyRange {
		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(user);
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(user);
		// The end key needs to be exclusive-upper, so we add a byte past the prefix
		let start_key = start.to_encoded_key();
		let mut end_bytes = end.to_encoded_key().to_vec();
		end_bytes.push(0xFF);
		EncodedKeyRange::start_end(Some(start_key), Some(EncodedKey::new(end_bytes)))
	}
}

impl EncodableKey for UserRoleKey {
	const KIND: KeyKind = KeyKind::UserRole;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer.extend_u8(VERSION).extend_u8(Self::KIND as u8).extend_u64(self.user).extend_u64(self.role);
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
		let user = de.read_u64().ok()?;
		let role = de.read_u64().ok()?;
		Some(Self {
			user,
			role,
		})
	}
}
