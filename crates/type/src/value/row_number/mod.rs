// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{fmt, ops::Deref};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

/// Standard column name for RowNumber columns
pub static ROW_NUMBER_COLUMN_NAME: &str = "__ROW__NUMBER__";

/// A row number - a unique 64-bit unsigned integer for a table row
#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Default)]
pub struct RowNumber(pub u64);

impl RowNumber {
	/// Create a new RowNumber from a u64
	pub fn new(id: u64) -> Self {
		RowNumber(id)
	}

	/// Get the inner u64 value
	pub fn value(&self) -> u64 {
		self.0
	}
}

impl Deref for RowNumber {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for RowNumber {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<u64> for RowNumber {
	fn from(id: u64) -> Self {
		RowNumber(id)
	}
}

impl From<RowNumber> for u64 {
	fn from(row_number: RowNumber) -> Self {
		row_number.0
	}
}

impl fmt::Display for RowNumber {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl Serialize for RowNumber {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for RowNumber {
	fn deserialize<D>(deserializer: D) -> Result<RowNumber, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = RowNumber;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(RowNumber(value))
			}
		}
		deserializer.deserialize_u64(U64Visitor)
	}
}
