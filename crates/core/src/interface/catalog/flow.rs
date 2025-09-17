// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{fmt, ops::Deref};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

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
