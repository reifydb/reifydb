// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{fmt, ops::Deref};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

/// Standard column name for RowId columns
pub static ROW_ID_COLUMN_NAME: &str = "__ROW__ID__";

/// A row identifier - a unique 64-bit unsigned integer for a table row
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Default)]
pub struct RowId(pub u64);

impl RowId {
	/// Create a new RowId from a u64
	pub fn new(id: u64) -> Self {
		RowId(id)
	}

	/// Get the inner u64 value
	pub fn value(&self) -> u64 {
		self.0
	}
}

impl Deref for RowId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for RowId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<u64> for RowId {
	fn from(id: u64) -> Self {
		RowId(id)
	}
}

impl From<RowId> for u64 {
	fn from(row_id: RowId) -> Self {
		row_id.0
	}
}

impl fmt::Display for RowId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl Serialize for RowId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for RowId {
	fn deserialize<D>(deserializer: D) -> Result<RowId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = RowId;

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
				Ok(RowId(value))
			}
		}
		deserializer.deserialize_u64(U64Visitor)
	}
}
