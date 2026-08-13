// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod family;
mod fingerprint;
mod values;

use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_value::value::value_type::ValueType;

#[test]
fn test_shape_creation() {
	let fields = vec![
		RowShapeField::unconstrained("id", ValueType::Int8),
		RowShapeField::unconstrained("name", ValueType::Utf8),
		RowShapeField::unconstrained("active", ValueType::Boolean),
	];

	let shape = RowShape::new(RowFamily::Table, fields);

	assert_eq!(shape.field_count(), 3);
	assert_eq!(shape.fields()[0].name, "id");
	assert_eq!(shape.fields()[1].name, "name");
	assert_eq!(shape.fields()[2].name, "active");
}

#[test]
fn test_shape_fingerprint_deterministic() {
	let fields1 = vec![
		RowShapeField::unconstrained("a", ValueType::Int4),
		RowShapeField::unconstrained("b", ValueType::Utf8),
	];

	let fields2 = vec![
		RowShapeField::unconstrained("a", ValueType::Int4),
		RowShapeField::unconstrained("b", ValueType::Utf8),
	];

	let shape1 = RowShape::new(RowFamily::Table, fields1);
	let shape2 = RowShape::new(RowFamily::Table, fields2);

	assert_eq!(shape1.fingerprint(), shape2.fingerprint());
}

#[test]
fn test_shape_fingerprint_different_for_different_shapes() {
	let fields1 = vec![RowShapeField::unconstrained("a", ValueType::Int4)];
	let fields2 = vec![RowShapeField::unconstrained("a", ValueType::Int8)];

	let shape1 = RowShape::new(RowFamily::Table, fields1);
	let shape2 = RowShape::new(RowFamily::Table, fields2);

	assert_ne!(shape1.fingerprint(), shape2.fingerprint());
}

#[test]
fn test_find_field() {
	let fields = vec![
		RowShapeField::unconstrained("id", ValueType::Int8),
		RowShapeField::unconstrained("name", ValueType::Utf8),
	];

	let shape = RowShape::new(RowFamily::Table, fields);

	assert!(shape.find_field("id").is_some());
	assert!(shape.find_field("name").is_some());
	assert!(shape.find_field("missing").is_none());
}
