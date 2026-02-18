// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SumTypeId(pub u64);

impl Display for SumTypeId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for SumTypeId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for SumTypeId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<SumTypeId> for u64 {
	fn from(value: SumTypeId) -> Self {
		value.0
	}
}

impl SumTypeId {
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for SumTypeId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for SumTypeId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for SumTypeId {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for SumTypeId {
	fn deserialize<D>(deserializer: D) -> Result<SumTypeId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = SumTypeId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(SumTypeId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}
