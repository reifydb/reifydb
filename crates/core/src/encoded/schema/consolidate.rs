// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema consolidation utilities - STUB
//!
//! This module will eventually provide functionality to:
//! - Consolidate multiple schemas into a single unified schema
//! - Widen types when merging incompatible field types
//!
//! Currently stubbed with unimplemented!() as this is out of scope
//! for the initial implementation.

use reifydb_runtime::hash::Hash64;
use reifydb_type::value::r#type::Type;

use super::Schema;

/// Type widening rules - STUB
///
/// Future: implement proper widening hierarchy (e.g., Int4 -> Int8 -> Int16)
pub fn widen_type(a: Type, b: Type) -> Type {
	match (a, b) {
		(Type::Undefined, t) | (t, Type::Undefined) => t,
		(a, b) if a == b => a,
		_ => unimplemented!("type widening not yet supported: {:?} vs {:?}", a, b),
	}
}

/// Find widest compatible schema from multiple fingerprints - STUB
///
/// Future: this will merge schemas by:
/// 1. Finding all unique field names
/// 2. For each field, finding the widest compatible type
/// 3. Producing a new schema that can represent all input schemas
#[allow(unused_variables)]
pub fn consolidate_schemas(fingerprints: &[Hash64], lookup: impl Fn(Hash64) -> Option<Schema>) -> Schema {
	unimplemented!("schema consolidation not yet supported")
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
		assert_eq!(widen_type(Type::Undefined, Type::Int4), Type::Int4);
		assert_eq!(widen_type(Type::Int4, Type::Undefined), Type::Int4);
	}

	#[test]
	#[should_panic(expected = "type widening not yet supported")]
	fn test_widen_type_different_panics() {
		widen_type(Type::Int4, Type::Utf8);
	}
}
