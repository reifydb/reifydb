// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnId(pub u64);

impl Deref for ColumnId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ColumnId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnId> for u64 {
	fn from(value: ColumnId) -> Self {
		value.0
	}
}

impl Serialize for ColumnId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ColumnId {
	fn deserialize<D>(deserializer: D) -> Result<ColumnId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ColumnId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ColumnId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub enum IndexId {
	Primary(PrimaryKeyId),
	// Future: Secondary, Unique, etc.
}

impl IndexId {
	pub fn as_u64(&self) -> u64 {
		match self {
			IndexId::Primary(id) => id.0,
		}
	}

	pub fn primary(id: impl Into<PrimaryKeyId>) -> Self {
		IndexId::Primary(id.into())
	}

	/// Creates a next index id for range operations (numerically next)
	pub fn next(&self) -> IndexId {
		match self {
			IndexId::Primary(primary) => IndexId::Primary(PrimaryKeyId(primary.0 + 1)), /* Future: handle
			                                                                             * other index
			                                                                             * types */
		}
	}

	pub fn prev(&self) -> IndexId {
		match self {
			IndexId::Primary(primary) => IndexId::Primary(PrimaryKeyId(primary.0.wrapping_sub(1))),
		}
	}
}

impl Deref for IndexId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		match self {
			IndexId::Primary(id) => &id.0,
		}
	}
}

impl PartialEq<u64> for IndexId {
	fn eq(&self, other: &u64) -> bool {
		self.as_u64().eq(other)
	}
}

impl From<IndexId> for u64 {
	fn from(value: IndexId) -> Self {
		value.as_u64()
	}
}

impl Serialize for IndexId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.as_u64())
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

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				// Deserialize as primary key ID for now
				Ok(IndexId::Primary(PrimaryKeyId(value)))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ColumnPropertyId(pub u64);

impl Deref for ColumnPropertyId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ColumnPropertyId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ColumnPropertyId> for u64 {
	fn from(value: ColumnPropertyId) -> Self {
		value.0
	}
}

impl Serialize for ColumnPropertyId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ColumnPropertyId {
	fn deserialize<D>(deserializer: D) -> Result<ColumnPropertyId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ColumnPropertyId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ColumnPropertyId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct NamespaceId(pub u64);

impl Display for NamespaceId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for NamespaceId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for NamespaceId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<NamespaceId> for u64 {
	fn from(value: NamespaceId) -> Self {
		value.0
	}
}

impl Serialize for NamespaceId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for NamespaceId {
	fn deserialize<D>(deserializer: D) -> Result<NamespaceId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = NamespaceId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(NamespaceId(value))
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

impl TableId {
	/// Get the inner u64 value.
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for TableId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for TableId {
	fn from(value: u64) -> Self {
		Self(value)
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

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
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

impl ViewId {
	/// Get the inner u64 value.
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for ViewId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for ViewId {
	fn from(value: u64) -> Self {
		Self(value)
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

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
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

impl From<i32> for PrimaryKeyId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for PrimaryKeyId {
	fn from(value: u64) -> Self {
		Self(value)
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

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(PrimaryKeyId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct RingBufferId(pub u64);

impl Display for RingBufferId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for RingBufferId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for RingBufferId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<RingBufferId> for u64 {
	fn from(value: RingBufferId) -> Self {
		value.0
	}
}

impl RingBufferId {
	/// Get the inner u64 value.
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for RingBufferId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for RingBufferId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for RingBufferId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for RingBufferId {
	fn deserialize<D>(deserializer: D) -> Result<RingBufferId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = RingBufferId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(RingBufferId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ProcedureId(pub u64);

impl Display for ProcedureId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for ProcedureId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for ProcedureId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<ProcedureId> for u64 {
	fn from(value: ProcedureId) -> Self {
		value.0
	}
}

impl From<i32> for ProcedureId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for ProcedureId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for ProcedureId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for ProcedureId {
	fn deserialize<D>(deserializer: D) -> Result<ProcedureId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = ProcedureId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(ProcedureId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

/// A unique identifier for a subscription.
/// Uses u64 for efficient storage and to unify with FlowId (FlowId == SubscriptionId for subscription flows).
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SubscriptionId(pub u64);

impl Display for SubscriptionId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SubscriptionId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SubscriptionId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SubscriptionId> for u64 {
	fn from(value: SubscriptionId) -> Self {
		value.0
	}
}

impl From<u64> for SubscriptionId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SubscriptionId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SubscriptionId {
	fn deserialize<D>(deserializer: D) -> Result<SubscriptionId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SubscriptionId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SubscriptionId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SequenceId(pub u64);

impl Deref for SequenceId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SequenceId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl Serialize for SequenceId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SequenceId {
	fn deserialize<D>(deserializer: D) -> Result<SequenceId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SequenceId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SequenceId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SubscriptionColumnId(pub u64);

impl Display for SubscriptionColumnId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SubscriptionColumnId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SubscriptionColumnId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SubscriptionColumnId> for u64 {
	fn from(value: SubscriptionColumnId) -> Self {
		value.0
	}
}

impl From<i32> for SubscriptionColumnId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for SubscriptionColumnId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SubscriptionColumnId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SubscriptionColumnId {
	fn deserialize<D>(deserializer: D) -> Result<SubscriptionColumnId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SubscriptionColumnId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SubscriptionColumnId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SeriesId(pub u64);

impl Display for SeriesId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SeriesId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SeriesId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SeriesId> for u64 {
	fn from(value: SeriesId) -> Self {
		value.0
	}
}

impl SeriesId {
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for SeriesId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for SeriesId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SeriesId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SeriesId {
	fn deserialize<D>(deserializer: D) -> Result<SeriesId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SeriesId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SeriesId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct HandlerId(pub u64);

impl Display for HandlerId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for HandlerId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for HandlerId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<HandlerId> for u64 {
	fn from(value: HandlerId) -> Self {
		value.0
	}
}

impl From<i32> for HandlerId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for HandlerId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for HandlerId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for HandlerId {
	fn deserialize<D>(deserializer: D) -> Result<HandlerId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = HandlerId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(HandlerId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct MigrationId(pub u64);

impl Display for MigrationId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for MigrationId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for MigrationId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<MigrationId> for u64 {
	fn from(value: MigrationId) -> Self {
		value.0
	}
}

impl From<i32> for MigrationId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for MigrationId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for MigrationId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for MigrationId {
	fn deserialize<D>(deserializer: D) -> Result<MigrationId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = MigrationId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(MigrationId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct MigrationEventId(pub u64);

impl Display for MigrationEventId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for MigrationEventId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for MigrationEventId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<MigrationEventId> for u64 {
	fn from(value: MigrationEventId) -> Self {
		value.0
	}
}

impl From<i32> for MigrationEventId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for MigrationEventId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for MigrationEventId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for MigrationEventId {
	fn deserialize<D>(deserializer: D) -> Result<MigrationEventId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = MigrationEventId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(MigrationEventId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}
