// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::hash::Hash64;
use reifydb_value::value::value_type::ValueType;

use super::RowShape;

pub fn widen_type(a: ValueType, b: ValueType) -> ValueType {
	match (a, b) {
		(ValueType::Option(_), t) | (t, ValueType::Option(_)) => t,
		(ref a, ref b) if a == b => a.clone(),
		(a, b) => unimplemented!("type widening not yet supported: {:?} vs {:?}", a, b),
	}
}

#[allow(unused_variables)]
pub fn consolidate_shapes(fingerprints: &[Hash64], lookup: impl Fn(Hash64) -> Option<RowShape>) -> RowShape {
	unimplemented!("shape consolidation not yet supported")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_widen_type_same() {
		assert_eq!(widen_type(ValueType::Int4, ValueType::Int4), ValueType::Int4);
	}

	#[test]
	fn test_widen_type_undefined() {
		assert_eq!(
			widen_type(ValueType::Option(Box::new(ValueType::Boolean)), ValueType::Int4),
			ValueType::Int4
		);
		assert_eq!(
			widen_type(ValueType::Int4, ValueType::Option(Box::new(ValueType::Boolean))),
			ValueType::Int4
		);
	}

	#[test]
	#[should_panic(expected = "type widening not yet supported")]
	fn test_widen_type_different_panics() {
		widen_type(ValueType::Int4, ValueType::Utf8);
	}
}
