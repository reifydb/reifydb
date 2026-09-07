// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::KeyCodec;
use reifydb_value::value::identity::IdentityId;

use super::KeyTag;
use crate::{
	interface::catalog::{
		authentication::AuthenticationId,
		identity::{IdentityAttributeId, RoleId},
		policy::PolicyId,
		token::TokenId,
	},
	key::{
		any::{ByteEncoding, Field, KeyFields, Width},
		bound::TaggedKeyBoundRange,
	},
};

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Identity)]
pub struct IdentityKey {
	pub identity: IdentityId,
}

impl IdentityKey {
	pub fn new(identity: IdentityId) -> Self {
		Self {
			identity,
		}
	}

	pub fn encoded(identity: IdentityId) -> EncodedKey {
		Self::new(identity).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod byte_identical_identity_key {
	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_value::value::uuid::Uuid7;
	use uuid::Uuid;

	use super::*;

	fn legacy_encode(key: &IdentityKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(KeyTag::Identity as u8).extend_identity_id(&key.identity);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for byte in [0u8, 1, 2] {
			let identity = IdentityId::from(Uuid7::from(Uuid::from_bytes([byte; 16])));
			let key = IdentityKey::new(identity);
			assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = IdentityAttribute)]
pub struct IdentityAttributeKey {
	pub attribute: IdentityAttributeId,
}

impl IdentityAttributeKey {
	pub fn new(attribute: IdentityAttributeId) -> Self {
		Self {
			attribute,
		}
	}

	pub fn encoded(attribute: IdentityAttributeId) -> EncodedKey {
		Self::new(attribute).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod byte_identical_identity_attribute_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::*;

	fn legacy_encode(key: &IdentityAttributeKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyTag::IdentityAttribute as u8).extend_u64(key.attribute);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for attribute in [0u64, 1, 42, u64::MAX] {
			let key = IdentityAttributeKey::new(attribute);
			assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = IdentityAttributeValue)]
pub struct IdentityAttributeValueKey {
	pub identity: IdentityId,
	pub attribute: IdentityAttributeId,
}

impl IdentityAttributeValueKey {
	pub fn new(identity: IdentityId, attribute: IdentityAttributeId) -> Self {
		Self {
			identity,
			attribute,
		}
	}

	pub fn encoded(identity: IdentityId, attribute: IdentityAttributeId) -> EncodedKey {
		Self::new(identity, attribute).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}

	pub fn identity_scan(identity: IdentityId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(
			Self::TAG,
			[Field::BytesDesc(ByteEncoding::Escaped, Cow::Owned(identity.as_bytes().to_vec()))],
		)
	}
}

#[cfg(test)]
mod byte_identical_identity_attribute_value_key {
	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_value::value::uuid::Uuid7;
	use uuid::Uuid;

	use super::*;

	fn legacy_encode(key: &IdentityAttributeValueKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer
			.extend_u8(KeyTag::IdentityAttributeValue as u8)
			.extend_identity_id(&key.identity)
			.extend_u64(key.attribute);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for byte in [0u8, 1, 2] {
			let identity = IdentityId::from(Uuid7::from(Uuid::from_bytes([byte; 16])));
			for attribute in [0u64, 1, u64::MAX] {
				let key = IdentityAttributeValueKey::new(identity, attribute);
				assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
			}
		}
	}
}

#[cfg(test)]
mod identity_attribute_value_key_tests {
	use std::ops::RangeBounds;

	use reifydb_value::value::{identity::IdentityId, uuid::Uuid7};
	use uuid::Uuid;

	use super::*;

	fn identity(byte: u8) -> IdentityId {
		IdentityId::from(Uuid7::from(Uuid::from_bytes([byte; 16])))
	}

	#[test]
	fn identity_scan_holds_every_attribute_of_that_identity() {
		// Small ids complement to a suffix starting 0xFF, so a prefix+0xFF end bound excludes them all.
		let alice = identity(1);
		let range = IdentityAttributeValueKey::identity_scan(alice).encode();

		for attribute in [0u64, 1, 2, u64::MAX] {
			let key = IdentityAttributeValueKey::encoded(alice, attribute);
			assert!(range.contains(&key), "attribute {attribute} must fall inside the identity scan");
		}
	}

	#[test]
	fn identity_scan_excludes_a_neighbouring_identity() {
		// The scan must not widen into the next identity when the bound is carry-incremented.
		let range = IdentityAttributeValueKey::identity_scan(identity(1)).encode();
		let other = IdentityAttributeValueKey::encoded(identity(2), 1);

		assert!(!range.contains(&other), "a different identity must stay outside the scan");
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Authentication)]
pub struct AuthenticationKey {
	pub authentication: AuthenticationId,
}

impl AuthenticationKey {
	pub fn new(authentication: AuthenticationId) -> Self {
		Self {
			authentication,
		}
	}

	pub fn encoded(authentication: AuthenticationId) -> EncodedKey {
		Self::new(authentication).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod byte_identical_authentication_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::*;

	fn legacy_encode(key: &AuthenticationKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyTag::Authentication as u8).extend_u64(key.authentication);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for authentication in [0u64, 1, 42, u64::MAX] {
			let key = AuthenticationKey::new(authentication);
			assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Token)]
pub struct TokenKey {
	pub token: TokenId,
}

impl TokenKey {
	pub fn new(token: TokenId) -> Self {
		Self {
			token,
		}
	}

	pub fn encoded(token: TokenId) -> EncodedKey {
		Self::new(token).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod byte_identical_token_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::*;

	fn legacy_encode(key: &TokenKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyTag::Token as u8).extend_u64(key.token);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for token in [0u64, 1, 42, u64::MAX] {
			let key = TokenKey::new(token);
			assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Role)]
pub struct RoleKey {
	pub role: RoleId,
}

impl RoleKey {
	pub fn new(role: RoleId) -> Self {
		Self {
			role,
		}
	}

	pub fn encoded(role: RoleId) -> EncodedKey {
		Self::new(role).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod byte_identical_role_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::*;

	fn legacy_encode(key: &RoleKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyTag::Role as u8).extend_u64(key.role);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for role in [0u64, 1, 42, u64::MAX] {
			let key = RoleKey::new(role);
			assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = GrantedRole)]
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

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}

	pub fn identity_scan(identity: IdentityId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(
			Self::TAG,
			[Field::BytesDesc(ByteEncoding::Escaped, Cow::Owned(identity.as_bytes().to_vec()))],
		)
	}
}

#[cfg(test)]
mod byte_identical_granted_role_key {
	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_value::value::uuid::Uuid7;
	use uuid::Uuid;

	use super::*;

	fn legacy_encode(key: &GrantedRoleKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer.extend_u8(KeyTag::GrantedRole as u8).extend_identity_id(&key.identity).extend_u64(key.role);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for byte in [0u8, 1, 2] {
			let identity = IdentityId::from(Uuid7::from(Uuid::from_bytes([byte; 16])));
			for role in [0u64, 1, u64::MAX] {
				let key = GrantedRoleKey::new(identity, role);
				assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
			}
		}
	}
}

#[cfg(test)]
mod granted_role_key_tests {
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
		let range = GrantedRoleKey::identity_scan(alice).encode();

		for role in [0u64, 1, 2, u64::MAX] {
			let key = GrantedRoleKey::encoded(alice, role);
			assert!(range.contains(&key), "role {role} must fall inside the identity scan");
		}
	}

	#[test]
	fn identity_scan_excludes_a_neighbouring_identity() {
		// The scan must not widen into the next identity when the bound is carry-incremented.
		let range = GrantedRoleKey::identity_scan(identity(1)).encode();

		assert!(!range.contains(&GrantedRoleKey::encoded(identity(2), 1)));
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Policy)]
pub struct PolicyKey {
	pub policy: PolicyId,
}

impl PolicyKey {
	pub fn new(policy: PolicyId) -> Self {
		Self {
			policy,
		}
	}

	pub fn encoded(policy: PolicyId) -> EncodedKey {
		Self::new(policy).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod byte_identical_policy_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::*;

	fn legacy_encode(key: &PolicyKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(KeyTag::Policy as u8).extend_u64(key.policy);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for policy in [0u64, 1, 42, u64::MAX] {
			let key = PolicyKey::new(policy);
			assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = PolicyOp)]
pub struct PolicyOpKey {
	pub policy: PolicyId,
	pub op_index: u64,
}

impl PolicyOpKey {
	pub fn new(policy: PolicyId, op_index: u64) -> Self {
		Self {
			policy,
			op_index,
		}
	}

	pub fn encoded(policy: PolicyId, op_index: u64) -> EncodedKey {
		Self::new(policy, op_index).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}

	pub fn policy_scan(policy: PolicyId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, policy as u128)])
	}
}

#[cfg(test)]
mod byte_identical_policy_op_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::*;

	fn legacy_encode(key: &PolicyOpKey) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(17);
		serializer.extend_u8(KeyTag::PolicyOp as u8).extend_u64(key.policy).extend_u64(key.op_index);
		serializer.to_encoded_key()
	}

	#[test]
	fn matches_the_flat_key_encoding() {
		for policy in [0u64, 1, u64::MAX] {
			for op_index in [0u64, 1, u64::MAX] {
				let key = PolicyOpKey::new(policy, op_index);
				assert_eq!(legacy_encode(&key).as_slice(), key.encode().as_slice());
			}
		}
	}
}

#[cfg(test)]
mod policy_op_key_tests {
	use std::ops::RangeBounds;

	use super::*;

	#[test]
	fn policy_scan_holds_every_op_index_of_that_policy() {
		// A fixed 0xFF-padded end bound only covers suffixes of exactly its own width.
		let range = PolicyOpKey::policy_scan(7).encode();

		for op_index in [0u64, 1, 2, u64::MAX] {
			let key = PolicyOpKey::encoded(7, op_index);
			assert!(range.contains(&key), "op index {op_index} must fall inside the policy scan");
		}
	}

	#[test]
	fn policy_scan_excludes_a_neighbouring_policy() {
		// The scan must not widen into the next policy when the bound is carry-incremented.
		let range = PolicyOpKey::policy_scan(7).encode();

		assert!(!range.contains(&PolicyOpKey::encoded(6, 1)));
		assert!(!range.contains(&PolicyOpKey::encoded(8, 1)));
	}
}
