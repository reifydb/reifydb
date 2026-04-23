// SPDX-License-Identifier: Apache-2.0
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

impl ColumnId {
	// request_history columns (IDs 1–8)
	pub const REQUEST_HISTORY_TIMESTAMP: ColumnId = ColumnId(1);
	pub const REQUEST_HISTORY_OPERATION: ColumnId = ColumnId(2);
	pub const REQUEST_HISTORY_FINGERPRINT: ColumnId = ColumnId(3);
	pub const REQUEST_HISTORY_TOTAL_DURATION: ColumnId = ColumnId(4);
	pub const REQUEST_HISTORY_COMPUTE_DURATION: ColumnId = ColumnId(5);
	pub const REQUEST_HISTORY_SUCCESS: ColumnId = ColumnId(6);
	pub const REQUEST_HISTORY_STATEMENT_COUNT: ColumnId = ColumnId(7);
	pub const REQUEST_HISTORY_NORMALIZED_RQL: ColumnId = ColumnId(8);

	// statement_stats columns (IDs 9–18)
	pub const STATEMENT_STATS_SNAPSHOT_TIMESTAMP: ColumnId = ColumnId(9);
	pub const STATEMENT_STATS_FINGERPRINT: ColumnId = ColumnId(10);
	pub const STATEMENT_STATS_NORMALIZED_RQL: ColumnId = ColumnId(11);
	pub const STATEMENT_STATS_CALLS: ColumnId = ColumnId(12);
	pub const STATEMENT_STATS_TOTAL_DURATION: ColumnId = ColumnId(13);
	pub const STATEMENT_STATS_MEAN_DURATION: ColumnId = ColumnId(14);
	pub const STATEMENT_STATS_MAX_DURATION: ColumnId = ColumnId(15);
	pub const STATEMENT_STATS_MIN_DURATION: ColumnId = ColumnId(16);
	pub const STATEMENT_STATS_TOTAL_ROWS: ColumnId = ColumnId(17);
	pub const STATEMENT_STATS_ERRORS: ColumnId = ColumnId(18);
}

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

impl RingBufferId {
	pub const REQUEST_HISTORY: RingBufferId = RingBufferId(1);
	pub const STATEMENT_STATS: RingBufferId = RingBufferId(2);
}

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
pub struct ProcedureId(u64);

impl ProcedureId {
	/// Lower bound of the id band reserved for ephemeral (Native/Ffi/Wasm) procedures.
	/// Persistent ids are strictly below this; ephemeral ids are at or above it.
	/// The split is enforced by the `persistent` / `ephemeral` constructors.
	pub const SYSTEM_RESERVED_START: u64 = 1 << 48;

	/// Reserved id for the built-in `system::config::set` Native procedure.
	/// Retained for backwards-compat references; ephemeral procedures now get
	/// fresh ids from a per-boot counter starting at `SYSTEM_RESERVED_START`.
	pub const SYSTEM_CONFIG_SET: ProcedureId = ProcedureId::persistent(1);

	/// Construct a persistent procedure id. Panics if `id >= SYSTEM_RESERVED_START`
	/// - that band is reserved for ephemeral (Native/Ffi/Wasm) procedures.
	pub const fn persistent(id: u64) -> Self {
		assert!(id < Self::SYSTEM_RESERVED_START, "persistent ProcedureId must be below SYSTEM_RESERVED_START");
		Self(id)
	}

	/// Construct an ephemeral procedure id. Panics if `id < SYSTEM_RESERVED_START`
	/// - that band belongs to persistent procedures.
	pub const fn ephemeral(id: u64) -> Self {
		assert!(
			id >= Self::SYSTEM_RESERVED_START,
			"ephemeral ProcedureId must be at or above SYSTEM_RESERVED_START"
		);
		Self(id)
	}

	/// Construct a `ProcedureId` from a raw `u64` without checking which band it
	/// falls in. Use this only for decoding trusted bytes (storage rows, key scans,
	/// deserialization) where the value has already been validated upstream.
	pub const fn from_raw(id: u64) -> Self {
		Self(id)
	}

	/// Returns `true` if this id was allocated from the ephemeral band.
	pub const fn is_ephemeral(&self) -> bool {
		self.0 >= Self::SYSTEM_RESERVED_START
	}
}

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
				Ok(ProcedureId::from_raw(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct TestId(pub u64);

impl Display for TestId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for TestId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for TestId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<TestId> for u64 {
	fn from(value: TestId) -> Self {
		value.0
	}
}

impl From<i32> for TestId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for TestId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for TestId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for TestId {
	fn deserialize<D>(deserializer: D) -> Result<TestId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = TestId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(TestId(value))
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

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SourceId(pub u64);

impl Display for SourceId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SourceId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SourceId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SourceId> for u64 {
	fn from(value: SourceId) -> Self {
		value.0
	}
}

impl From<u64> for SourceId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SourceId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SourceId {
	fn deserialize<D>(deserializer: D) -> Result<SourceId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SourceId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SourceId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct BindingId(pub u64);

impl Display for BindingId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for BindingId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for BindingId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<BindingId> for u64 {
	fn from(value: BindingId) -> Self {
		value.0
	}
}

impl From<u64> for BindingId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for BindingId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for BindingId {
	fn deserialize<D>(deserializer: D) -> Result<BindingId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = BindingId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(BindingId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SinkId(pub u64);

impl Display for SinkId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SinkId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SinkId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SinkId> for u64 {
	fn from(value: SinkId) -> Self {
		value.0
	}
}

impl From<u64> for SinkId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SinkId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SinkId {
	fn deserialize<D>(deserializer: D) -> Result<SinkId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SinkId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SinkId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}
