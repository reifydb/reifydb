// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::hash::Hash64;
use reifydb_type::value::r#type::Type;

use super::RowShape;

pub fn widen_type(a: Type, b: Type) -> Type {
	match (a, b) {
		(Type::Option(_), t) | (t, Type::Option(_)) => t,
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
		assert_eq!(widen_type(Type::Int4, Type::Int4), Type::Int4);
	}

	#[test]
	fn test_widen_type_undefined() {
		assert_eq!(widen_type(Type::Option(Box::new(Type::Boolean)), Type::Int4), Type::Int4);
		assert_eq!(widen_type(Type::Int4, Type::Option(Box::new(Type::Boolean))), Type::Int4);
	}

	#[test]
	#[should_panic(expected = "type widening not yet supported")]
	fn test_widen_type_different_panics() {
		widen_type(Type::Int4, Type::Utf8);
	}
}
