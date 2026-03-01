// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	cmp, fmt,
	fmt::{Display, Formatter},
	ops::Deref,
	result::Result as StdResult,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

use super::{Value, r#type::Type};
use crate::{Result, error::TypeError};

/// A dictionary entry ID that can be one of several unsigned integer sizes.
/// The variant used depends on the dictionary's `id_type` configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DictionaryEntryId {
	U1(u8),
	U2(u16),
	U4(u32),
	U8(u64),
	U16(u128),
}

impl DictionaryEntryId {
	/// Create a DictionaryEntryId from a u128 value and the target Type.
	/// Returns an error if the value doesn't fit in the specified type.
	pub fn from_u128(value: u128, id_type: Type) -> Result<Self> {
		match id_type {
			Type::Uint1 => {
				if value > u8::MAX as u128 {
					return Err(TypeError::DictionaryCapacityExceeded {
						id_type: Type::Uint1,
						value,
						max_value: u8::MAX as u128,
					}
					.into());
				}
				Ok(Self::U1(value as u8))
			}
			Type::Uint2 => {
				if value > u16::MAX as u128 {
					return Err(TypeError::DictionaryCapacityExceeded {
						id_type: Type::Uint2,
						value,
						max_value: u16::MAX as u128,
					}
					.into());
				}
				Ok(Self::U2(value as u16))
			}
			Type::Uint4 => {
				if value > u32::MAX as u128 {
					return Err(TypeError::DictionaryCapacityExceeded {
						id_type: Type::Uint4,
						value,
						max_value: u32::MAX as u128,
					}
					.into());
				}
				Ok(Self::U4(value as u32))
			}
			Type::Uint8 => {
				if value > u64::MAX as u128 {
					return Err(TypeError::DictionaryCapacityExceeded {
						id_type: Type::Uint8,
						value,
						max_value: u64::MAX as u128,
					}
					.into());
				}
				Ok(Self::U8(value as u64))
			}
			Type::Uint16 => Ok(Self::U16(value)),
			// FIXME replace me with error
			_ => unimplemented!(
				"Invalid dictionary id_type: {:?}. Must be Uint1, Uint2, Uint4, Uint8, or Uint16",
				id_type
			),
		}
	}

	/// Convert this ID to a u128 value for internal storage/computation.
	pub fn to_u128(&self) -> u128 {
		match self {
			Self::U1(v) => *v as u128,
			Self::U2(v) => *v as u128,
			Self::U4(v) => *v as u128,
			Self::U8(v) => *v as u128,
			Self::U16(v) => *v,
		}
	}

	/// Get the Type this ID represents.
	pub fn id_type(&self) -> Type {
		match self {
			Self::U1(_) => Type::Uint1,
			Self::U2(_) => Type::Uint2,
			Self::U4(_) => Type::Uint4,
			Self::U8(_) => Type::Uint8,
			Self::U16(_) => Type::Uint16,
		}
	}

	/// Convert this DictionaryEntryId to a Value.
	pub fn to_value(self) -> Value {
		Value::DictionaryId(self)
	}

	/// Create a DictionaryEntryId from a Value.
	/// Returns None if the Value is not a DictionaryId or unsigned integer type.
	pub fn from_value(value: &Value) -> Option<Self> {
		match value {
			Value::DictionaryId(id) => Some(*id),
			Value::Uint1(v) => Some(Self::U1(*v)),
			Value::Uint2(v) => Some(Self::U2(*v)),
			Value::Uint4(v) => Some(Self::U4(*v)),
			Value::Uint8(v) => Some(Self::U8(*v)),
			Value::Uint16(v) => Some(Self::U16(*v)),
			_ => None,
		}
	}
}

impl PartialOrd for DictionaryEntryId {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for DictionaryEntryId {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		self.to_u128().cmp(&other.to_u128())
	}
}

impl Default for DictionaryEntryId {
	fn default() -> Self {
		Self::U1(0)
	}
}

impl fmt::Display for DictionaryEntryId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::U1(v) => write!(f, "{}", v),
			Self::U2(v) => write!(f, "{}", v),
			Self::U4(v) => write!(f, "{}", v),
			Self::U8(v) => write!(f, "{}", v),
			Self::U16(v) => write!(f, "{}", v),
		}
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct DictionaryId(pub u64);

impl Display for DictionaryId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Deref for DictionaryId {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialEq<u64> for DictionaryId {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<DictionaryId> for u64 {
	fn from(value: DictionaryId) -> Self {
		value.0
	}
}

impl DictionaryId {
	/// Get the inner u64 value.
	#[inline]
	pub fn to_u64(self) -> u64 {
		self.0
	}
}

impl From<i32> for DictionaryId {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for DictionaryId {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for DictionaryId {
	fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for DictionaryId {
	fn deserialize<D>(deserializer: D) -> StdResult<DictionaryId, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = DictionaryId;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> StdResult<Self::Value, E> {
				Ok(DictionaryId(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_from_u128_u1() {
		let id = DictionaryEntryId::from_u128(42, Type::Uint1).unwrap();
		assert_eq!(id, DictionaryEntryId::U1(42));
		assert_eq!(id.to_u128(), 42);
		assert_eq!(id.id_type(), Type::Uint1);
	}

	#[test]
	fn test_from_u128_u1_overflow() {
		let err = DictionaryEntryId::from_u128(256, Type::Uint1).unwrap_err();
		assert!(err.to_string().contains("DICT_001"));
	}

	#[test]
	fn test_from_u128_u2() {
		let id = DictionaryEntryId::from_u128(1000, Type::Uint2).unwrap();
		assert_eq!(id, DictionaryEntryId::U2(1000));
		assert_eq!(id.to_u128(), 1000);
	}

	#[test]
	fn test_from_u128_u4() {
		let id = DictionaryEntryId::from_u128(100_000, Type::Uint4).unwrap();
		assert_eq!(id, DictionaryEntryId::U4(100_000));
		assert_eq!(id.to_u128(), 100_000);
	}

	#[test]
	fn test_from_u128_u8() {
		let id = DictionaryEntryId::from_u128(10_000_000_000, Type::Uint8).unwrap();
		assert_eq!(id, DictionaryEntryId::U8(10_000_000_000));
		assert_eq!(id.to_u128(), 10_000_000_000);
	}

	#[test]
	fn test_from_u128_u16() {
		let large_value: u128 = u64::MAX as u128 + 1;
		let id = DictionaryEntryId::from_u128(large_value, Type::Uint16).unwrap();
		assert_eq!(id, DictionaryEntryId::U16(large_value));
		assert_eq!(id.to_u128(), large_value);
	}

	#[test]
	fn test_display() {
		assert_eq!(format!("{}", DictionaryEntryId::U1(42)), "42");
		assert_eq!(format!("{}", DictionaryEntryId::U8(12345)), "12345");
	}
}
