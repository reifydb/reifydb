// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowShape evolution and resolution utilities.
//!
//! ShapeResolver handles reading data written with one shape
//! using a different (but compatible) target shape.

use reifydb_type::value::constraint::TypeConstraint;

use crate::encoded::shape::RowShape;

/// Describes how to map a source field to a target field during shape evolution.
#[derive(Debug, Clone)]
pub enum FieldMapping {
	/// Field exists in both shapes at the given source index
	Direct {
		source_index: usize,
	},
	/// Field is new in target shape, use default value
	UseDefault,
	/// Field was removed (source has it, target doesn't) - skip during read
	Removed,
}

/// Resolves differences between source and target shapes.
///
/// Used when reading data that was written with an older shape version
/// using a newer shape, or vice versa.
#[derive(Debug)]
pub struct ShapeResolver {
	/// The shape the data was written with
	source: RowShape,
	/// The shape we want to read as
	target: RowShape,
	/// Mapping from target field index to source field
	mappings: Vec<FieldMapping>,
}

impl ShapeResolver {
	/// Create a resolver to read data from source shape as target shape.
	///
	/// Returns None if the shapes are incompatible (e.g., type mismatch
	/// on same-named field without valid widening path).
	pub fn new(source: RowShape, target: RowShape) -> Option<Self> {
		// If fingerprints match, no resolution needed - shapes are identical
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
			if let Some((shape_idx, source_field)) =
				source.fields().iter().enumerate().find(|(_, f)| f.name == target_field.name)
			{
				// Field exists in both - check type compatibility
				if !Self::types_compatible(&source_field.constraint, &target_field.constraint) {
					return None; // Incompatible types
				}
				mappings.push(FieldMapping::Direct {
					source_index: shape_idx,
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
		let shape_type = source.get_type();
		let target_type = target.get_type();

		if shape_type == target_type {
			return true;
		}

		// Type widening would go here
		// For now, only identical types are compatible
		false
	}

	/// Get the source shape
	pub fn source(&self) -> &RowShape {
		&self.source
	}

	/// Get the target shape
	pub fn target(&self) -> &RowShape {
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
	use crate::encoded::shape::RowShapeField;

	#[test]
	fn test_resolver_identity() {
		let fields = vec![
			RowShapeField::unconstrained("a", Type::Int4),
			RowShapeField::unconstrained("b", Type::Utf8),
		];

		let shape = RowShape::new(fields);
		let resolver = ShapeResolver::new(shape.clone(), shape.clone()).unwrap();

		assert!(resolver.is_identity());
		assert_eq!(resolver.mappings().len(), 2);
	}

	#[test]
	fn test_resolver_added_field() {
		let source_fields = vec![RowShapeField::unconstrained("a", Type::Int4)];

		let target_fields = vec![
			RowShapeField::unconstrained("a", Type::Int4),
			RowShapeField::unconstrained("b", Type::Utf8), // new field
		];

		let source = RowShape::new(source_fields);
		let target = RowShape::new(target_fields);

		let resolver = ShapeResolver::new(source, target).unwrap();

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
		let source_fields = vec![RowShapeField::unconstrained("a", Type::Int4)];
		let target_fields = vec![RowShapeField::unconstrained("a", Type::Utf8)]; // type changed

		let source = RowShape::new(source_fields);
		let target = RowShape::new(target_fields);

		// Should return None due to incompatible types
		assert!(ShapeResolver::new(source, target).is_none());
	}
}
