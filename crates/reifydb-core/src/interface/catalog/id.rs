// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	fmt,
	fmt::{Display, Formatter},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct TableColumnId(pub u64);

impl Deref for TableColumnId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for TableColumnId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<TableColumnId> for u64 {
	fn from(value: TableColumnId) -> Self {
		value.0
	}
}

impl Serialize for TableColumnId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for TableColumnId {
	fn deserialize<D>(deserializer: D) -> Result<TableColumnId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = TableColumnId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(
				self,
				value: u64,
			) -> Result<Self::Value, E> {
				Ok(TableColumnId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ViewColumnId(pub u64);

impl Deref for ViewColumnId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ViewColumnId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ViewColumnId> for u64 {
	fn from(value: ViewColumnId) -> Self {
		value.0
	}
}

impl Serialize for ViewColumnId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ViewColumnId {
	fn deserialize<D>(deserializer: D) -> Result<ViewColumnId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ViewColumnId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(
				self,
				value: u64,
			) -> Result<Self::Value, E> {
				Ok(ViewColumnId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct IndexId(pub u64);

impl Deref for IndexId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for IndexId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<IndexId> for u64 {
	fn from(value: IndexId) -> Self {
		value.0
	}
}

impl Serialize for IndexId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for IndexId {
	fn deserialize<D>(deserializer: D) -> Result<IndexId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = IndexId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(
				self,
				value: u64,
			) -> Result<Self::Value, E> {
				Ok(IndexId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnPolicyId(pub u64);

impl Deref for ColumnPolicyId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ColumnPolicyId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnPolicyId> for u64 {
	fn from(value: ColumnPolicyId) -> Self {
		value.0
	}
}

impl Serialize for ColumnPolicyId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ColumnPolicyId {
	fn deserialize<D>(deserializer: D) -> Result<ColumnPolicyId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ColumnPolicyId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(
				self,
				value: u64,
			) -> Result<Self::Value, E> {
				Ok(ColumnPolicyId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SchemaId(pub u64);

impl Display for SchemaId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SchemaId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SchemaId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SchemaId> for u64 {
	fn from(value: SchemaId) -> Self {
		value.0
	}
}

impl Serialize for SchemaId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SchemaId {
	fn deserialize<D>(deserializer: D) -> Result<SchemaId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SchemaId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(
				self,
				value: u64,
			) -> Result<Self::Value, E> {
				Ok(SchemaId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct TableId(pub u64);

impl Display for TableId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for TableId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for TableId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<TableId> for u64 {
	fn from(value: TableId) -> Self {
		value.0
	}
}

impl Serialize for TableId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for TableId {
	fn deserialize<D>(deserializer: D) -> Result<TableId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = TableId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(
				self,
				value: u64,
			) -> Result<Self::Value, E> {
				Ok(TableId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ViewId(pub u64);

impl Display for ViewId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for ViewId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ViewId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ViewId> for u64 {
	fn from(value: ViewId) -> Self {
		value.0
	}
}

impl Serialize for ViewId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ViewId {
	fn deserialize<D>(deserializer: D) -> Result<ViewId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ViewId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(
				self,
				value: u64,
			) -> Result<Self::Value, E> {
				Ok(ViewId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct PrimaryKeyId(pub u64);

impl Display for PrimaryKeyId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for PrimaryKeyId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for PrimaryKeyId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<PrimaryKeyId> for u64 {
	fn from(value: PrimaryKeyId) -> Self {
		value.0
	}
}

impl Serialize for PrimaryKeyId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for PrimaryKeyId {
	fn deserialize<D>(deserializer: D) -> Result<PrimaryKeyId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = PrimaryKeyId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(
				self,
				value: u64,
			) -> Result<Self::Value, E> {
				Ok(PrimaryKeyId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SystemSequenceId(pub u32);

impl Deref for SystemSequenceId {
	type Target = u32;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u32> for SystemSequenceId {
	fn eq(&self, other: &u32) -> bool {
		self.0.eq(other)
	}
}

impl Serialize for SystemSequenceId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u32(self.0)
	}
}

impl<'de> Deserialize<'de> for SystemSequenceId {
	fn deserialize<D>(deserializer: D) -> Result<SystemSequenceId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U32Visitor;

		impl Visitor<'_> for U32Visitor {
			type Value = SystemSequenceId;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str("an unsigned 32-bit number")
			}

			fn visit_u32<E>(
				self,
				value: u32,
			) -> Result<Self::Value, E> {
				Ok(SystemSequenceId(value))
			}
		}

		deserializer.deserialize_u32(U32Visitor)
	}
}
