// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use diagnostic::dictionary::dictionary_entry_id_capacity_exceeded;
use serde::{Deserialize, Serialize};

use super::Type;
use crate::{diagnostic, error, internal_error};

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
	pub fn from_u128(value: u128, id_type: Type) -> crate::Result<Self> {
		match id_type {
			Type::Uint1 => {
				if value > u8::MAX as u128 {
					return Err(error!(dictionary_entry_id_capacity_exceeded(
						Type::Uint1,
						value,
						u8::MAX as u128
					)));
				}
				Ok(Self::U1(value as u8))
			}
			Type::Uint2 => {
				if value > u16::MAX as u128 {
					return Err(error!(dictionary_entry_id_capacity_exceeded(
						Type::Uint2,
						value,
						u16::MAX as u128
					)));
				}
				Ok(Self::U2(value as u16))
			}
			Type::Uint4 => {
				if value > u32::MAX as u128 {
					return Err(error!(dictionary_entry_id_capacity_exceeded(
						Type::Uint4,
						value,
						u32::MAX as u128
					)));
				}
				Ok(Self::U4(value as u32))
			}
			Type::Uint8 => {
				if value > u64::MAX as u128 {
					return Err(error!(dictionary_entry_id_capacity_exceeded(
						Type::Uint8,
						value,
						u64::MAX as u128
					)));
				}
				Ok(Self::U8(value as u64))
			}
			Type::Uint16 => Ok(Self::U16(value)),
			_ => Err(internal_error!(
				"Invalid dictionary id_type: {:?}. Must be Uint1, Uint2, Uint4, Uint8, or Uint16",
				id_type
			)),
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
	pub fn to_value(self) -> super::Value {
		match self {
			Self::U1(v) => super::Value::Uint1(v),
			Self::U2(v) => super::Value::Uint2(v),
			Self::U4(v) => super::Value::Uint4(v),
			Self::U8(v) => super::Value::Uint8(v),
			Self::U16(v) => super::Value::Uint16(v),
		}
	}

	/// Create a DictionaryEntryId from a Value.
	/// Returns None if the Value is not an unsigned integer type.
	pub fn from_value(value: &super::Value) -> Option<Self> {
		match value {
			super::Value::Uint1(v) => Some(Self::U1(*v)),
			super::Value::Uint2(v) => Some(Self::U2(*v)),
			super::Value::Uint4(v) => Some(Self::U4(*v)),
			super::Value::Uint8(v) => Some(Self::U8(*v)),
			super::Value::Uint16(v) => Some(Self::U16(*v)),
			_ => None,
		}
	}
}

impl std::fmt::Display for DictionaryEntryId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::U1(v) => write!(f, "{}", v),
			Self::U2(v) => write!(f, "{}", v),
			Self::U4(v) => write!(f, "{}", v),
			Self::U8(v) => write!(f, "{}", v),
			Self::U16(v) => write!(f, "{}", v),
		}
	}
}

#[cfg(test)]
mod tests {
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
	fn test_invalid_type() {
		let err = DictionaryEntryId::from_u128(42, Type::Utf8).unwrap_err();
		assert!(err.to_string().contains("Invalid dictionary id_type"));
	}

	#[test]
	fn test_display() {
		assert_eq!(format!("{}", DictionaryEntryId::U1(42)), "42");
		assert_eq!(format!("{}", DictionaryEntryId::U8(12345)), "12345");
	}
}
