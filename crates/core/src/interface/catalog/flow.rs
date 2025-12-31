// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
	ops::Deref,
};

use reifydb_type::Blob;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

use crate::interface::NamespaceId;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct FlowId(pub u64);

impl Deref for FlowId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for FlowId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<FlowId> for u64 {
	fn from(value: FlowId) -> Self {
		value.0
	}
}

impl From<u64> for FlowId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for FlowId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for FlowId {
	fn deserialize<D>(deserializer: D) -> Result<FlowId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = FlowId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(FlowId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct FlowNodeId(pub u64);

impl Display for FlowNodeId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for FlowNodeId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for FlowNodeId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<FlowNodeId> for u64 {
	fn from(value: FlowNodeId) -> Self {
		value.0
	}
}

impl From<&FlowNodeId> for FlowNodeId {
	fn from(value: &FlowNodeId) -> Self {
		*value
	}
}

impl From<u64> for FlowNodeId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for FlowNodeId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for FlowNodeId {
	fn deserialize<D>(deserializer: D) -> Result<FlowNodeId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = FlowNodeId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(FlowNodeId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct FlowEdgeId(pub u64);

impl Deref for FlowEdgeId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for FlowEdgeId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<FlowEdgeId> for u64 {
	fn from(value: FlowEdgeId) -> Self {
		value.0
	}
}

impl From<u64> for FlowEdgeId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for FlowEdgeId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for FlowEdgeId {
	fn deserialize<D>(deserializer: D) -> Result<FlowEdgeId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = FlowEdgeId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(FlowEdgeId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FlowStatus {
	Active,
	Paused,
	Failed,
}

impl FlowStatus {
	/// Convert FlowStatus to u8 for storage
	pub fn to_u8(self) -> u8 {
		match self {
			FlowStatus::Active => 0,
			FlowStatus::Paused => 1,
			FlowStatus::Failed => 2,
		}
	}

	/// Create FlowStatus from u8, defaulting to Failed for unknown values
	pub fn from_u8(value: u8) -> Self {
		match value {
			0 => FlowStatus::Active,
			1 => FlowStatus::Paused,
			2 => FlowStatus::Failed,
			_ => FlowStatus::Failed, // Default to Failed for unknown statuses
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowDef {
	pub id: FlowId,
	pub namespace: NamespaceId,
	pub name: String,
	pub status: FlowStatus,
}

/// Catalog definition for a flow node
/// The node type and its data are stored as a type discriminator and serialized blob
#[derive(Debug, Clone, PartialEq)]
pub struct FlowNodeDef {
	pub id: FlowNodeId,
	pub flow: FlowId,
	pub node_type: u8, // FlowNodeType discriminator
	pub data: Blob,    // Serialized FlowNodeType data
}

/// Catalog definition for a flow edge
#[derive(Debug, Clone, PartialEq)]
pub struct FlowEdgeDef {
	pub id: FlowEdgeId,
	pub flow: FlowId,
	pub source: FlowNodeId,
	pub target: FlowNodeId,
}
