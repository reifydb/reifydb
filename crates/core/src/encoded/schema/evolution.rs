// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema evolution and resolution utilities.
//!
//! SchemaResolver handles reading data written with one schema
//! using a different (but compatible) target schema.

use std::sync::Arc;

use reifydb_type::value::constraint::TypeConstraint;

use crate::encoded::schema::Schema;

/// Describes how to map a source field to a target field during schema evolution.
#[derive(Debug, Clone)]
pub enum FieldMapping {
	/// Field exists in both schemas at the given source index
	Direct {
		source_index: usize,
	},
	/// Field is new in target schema, use default value
	UseDefault,
	/// Field was removed (source has it, target doesn't) - skip during read
	Removed,
}

/// Resolves differences between source and target schemas.
///
/// Used when reading data that was written with an older schema version
/// using a newer schema, or vice versa.
#[derive(Debug)]
pub struct SchemaResolver {
	/// The schema the data was written with
	source: Arc<Schema>,
	/// The schema we want to read as
	target: Arc<Schema>,
	/// Mapping from target field index to source field
	mappings: Vec<FieldMapping>,
}

impl SchemaResolver {
	/// Create a resolver to read data from source schema as target schema.
	///
	/// Returns None if the schemas are incompatible (e.g., type mismatch
	/// on same-named field without valid widening path).
	pub fn new(source: Arc<Schema>, target: Arc<Schema>) -> Option<Self> {
		// If fingerprints match, no resolution needed - schemas are identical
		if source.fingerprint() == target.fingerprint() {
			return Some(Self {
				mappings: (0..target.field_count())
					.map(|i| FieldMapping::Direct {
						source_index: i,
					})
					.collect(),
				source,
				target,
			});
		}

		let mut mappings = Vec::with_capacity(target.field_count());

		for target_field in target.fields() {
			if let Some((source_idx, source_field)) =
				source.fields().iter().enumerate().find(|(_, f)| f.name == target_field.name)
			{
				// Field exists in both - check type compatibility
				if !Self::types_compatible(&source_field.constraint, &target_field.constraint) {
					return None; // Incompatible types
				}
				mappings.push(FieldMapping::Direct {
					source_index: source_idx,
				});
			} else {
				// Field only in target - needs default
				mappings.push(FieldMapping::UseDefault);
			}
		}

		Some(Self {
			source,
			target,
			mappings,
		})
	}

	/// Check if source constraint can be read as target constraint.
	/// For now, just compares base types - constraint widening could be added later.
	fn types_compatible(source: &TypeConstraint, target: &TypeConstraint) -> bool {
		let source_type = source.get_type();
		let target_type = target.get_type();

		if source_type == target_type {
			return true;
		}

		// Type widening would go here
		// For now, only identical types are compatible
		false
	}

	/// Get the source schema
	pub fn source(&self) -> &Schema {
		&self.source
	}

	/// Get the target schema
	pub fn target(&self) -> &Schema {
		&self.target
	}

	/// Get the field mappings
	pub fn mappings(&self) -> &[FieldMapping] {
		&self.mappings
	}

	/// Check if this is an identity mapping (source == target)
	pub fn is_identity(&self) -> bool {
		self.source.fingerprint() == self.target.fingerprint()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::encoded::schema::SchemaField;

	#[test]
	fn test_resolver_identity() {
		let fields =
			vec![SchemaField::unconstrained("a", Type::Int4), SchemaField::unconstrained("b", Type::Utf8)];

		let schema = Arc::new(Schema::new(fields));
		let resolver = SchemaResolver::new(schema.clone(), schema.clone()).unwrap();

		assert!(resolver.is_identity());
		assert_eq!(resolver.mappings().len(), 2);
	}

	#[test]
	fn test_resolver_added_field() {
		let source_fields = vec![SchemaField::unconstrained("a", Type::Int4)];

		let target_fields = vec![
			SchemaField::unconstrained("a", Type::Int4),
			SchemaField::unconstrained("b", Type::Utf8), // new field
		];

		let source = Arc::new(Schema::new(source_fields));
		let target = Arc::new(Schema::new(target_fields));

		let resolver = SchemaResolver::new(source, target).unwrap();

		assert!(!resolver.is_identity());
		assert!(matches!(
			resolver.mappings()[0],
			FieldMapping::Direct {
				source_index: 0
			}
		));
		assert!(matches!(resolver.mappings()[1], FieldMapping::UseDefault));
	}

	#[test]
	fn test_resolver_incompatible_types() {
		let source_fields = vec![SchemaField::unconstrained("a", Type::Int4)];
		let target_fields = vec![SchemaField::unconstrained("a", Type::Utf8)]; // type changed

		let source = Arc::new(Schema::new(source_fields));
		let target = Arc::new(Schema::new(target_fields));

		// Should return None due to incompatible types
		assert!(SchemaResolver::new(source, target).is_none());
	}
}
