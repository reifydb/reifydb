// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{fmt, ops::Deref};

use reifydb_type::value::blob::Blob;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

use crate::interface::catalog::id::NamespaceId;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ReducerId(pub u64);

impl Deref for ReducerId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ReducerId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ReducerId> for u64 {
	fn from(value: ReducerId) -> Self {
		value.0
	}
}

impl ReducerId {
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<u64> for ReducerId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for ReducerId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ReducerId {
	fn deserialize<D>(deserializer: D) -> Result<ReducerId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ReducerId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ReducerId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ReducerActionId(pub u64);

impl Deref for ReducerActionId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ReducerActionId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ReducerActionId> for u64 {
	fn from(value: ReducerActionId) -> Self {
		value.0
	}
}

impl ReducerActionId {
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<u64> for ReducerActionId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for ReducerActionId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ReducerActionId {
	fn deserialize<D>(deserializer: D) -> Result<ReducerActionId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ReducerActionId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ReducerActionId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReducerDef {
	pub id: ReducerId,
	pub namespace: NamespaceId,
	pub name: String,
	pub key_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReducerActionDef {
	pub id: ReducerActionId,
	pub reducer: ReducerId,
	pub name: String,
	pub data: Blob,
}
