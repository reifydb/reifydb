// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{Value, decimal::Decimal, int::Int, r#type::Type, uint::Uint};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BigNum {
	Int(Int),
	Uint(Uint),
	Decimal(Decimal),
}

impl From<Value> for BigNum {
	fn from(value: Value) -> Self {
		match value {
			Value::Int(v) => BigNum::Int(v),
			Value::Uint(v) => BigNum::Uint(v),
			Value::Decimal(v) => BigNum::Decimal(v),
			other => unreachable!("BigNum::from: non-bignum Value variant: {other:?}"),
		}
	}
}

// Storage for variable-precision numeric types (`Int`, `Uint`, `Decimal`).
// v1 holds owned `BigNum`s; compressed BigNum encodings are stubbed, so the
// canonical form doesn't need a specialized layout yet.
#[derive(Clone, Debug)]
pub struct BigNumArray {
	pub ty: Type,
	pub values: Vec<BigNum>,
}

impl BigNumArray {
	pub fn new(ty: Type) -> Self {
		Self {
			ty,
			values: Vec::new(),
		}
	}

	pub fn from_values(ty: Type, values: Vec<BigNum>) -> Self {
		Self {
			ty,
			values,
		}
	}

	pub fn len(&self) -> usize {
		self.values.len()
	}

	pub fn is_empty(&self) -> bool {
		self.values.is_empty()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn from_values_preserves_variants() {
		let values = vec![
			BigNum::Int(Int::from_i64(-7)),
			BigNum::Uint(Uint::from_u64(42)),
			BigNum::Decimal(Decimal::from_i64(9)),
		];
		let ba = BigNumArray::from_values(Type::Int, values);
		assert_eq!(ba.len(), 3);
		assert!(matches!(ba.values[0], BigNum::Int(_)));
		assert!(matches!(ba.values[1], BigNum::Uint(_)));
		assert!(matches!(ba.values[2], BigNum::Decimal(_)));
	}

	#[test]
	fn from_value_maps_bignum_variants() {
		assert!(matches!(BigNum::from(Value::Int(Int::from_i64(1))), BigNum::Int(_)));
		assert!(matches!(BigNum::from(Value::Uint(Uint::from_u64(1))), BigNum::Uint(_)));
		assert!(matches!(BigNum::from(Value::Decimal(Decimal::from_i64(1))), BigNum::Decimal(_)));
	}
}
