// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::constraint::TypeConstraint;

use crate::encoded::shape::RowShape;

#[derive(Debug, Clone)]
pub enum FieldMapping {
	Direct {
		source_index: usize,
	},

	UseDefault,

	Removed,
}

#[derive(Debug)]
pub struct ShapeResolver {
	source: RowShape,

	target: RowShape,

	mappings: Vec<FieldMapping>,
}

impl ShapeResolver {
	pub fn new(source: RowShape, target: RowShape) -> Option<Self> {
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
				if !Self::types_compatible(&source_field.constraint, &target_field.constraint) {
					return None;
				}
				mappings.push(FieldMapping::Direct {
					source_index: shape_idx,
				});
			} else {
				mappings.push(FieldMapping::UseDefault);
			}
		}

		Some(Self {
			source,
			target,
			mappings,
		})
	}

	fn types_compatible(source: &TypeConstraint, target: &TypeConstraint) -> bool {
		let shape_type = source.get_type();
		let target_type = target.get_type();

		if shape_type == target_type {
			return true;
		}

		false
	}

	pub fn source(&self) -> &RowShape {
		&self.source
	}

	pub fn target(&self) -> &RowShape {
		&self.target
	}

	pub fn mappings(&self) -> &[FieldMapping] {
		&self.mappings
	}

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
