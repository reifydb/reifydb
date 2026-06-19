// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const PRIMITIVE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const VALUE: usize = 3;
	pub(crate) const INDEX: usize = 4;
	pub(crate) const AUTO_INCREMENT: usize = 5;
	pub(crate) const CONSTRAINT: usize = 6;
	pub(crate) const DICTIONARY_ID: usize = 7;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("primitive", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("value", ValueType::Uint1),
			RowShapeField::unconstrained("index", ValueType::Uint1),
			RowShapeField::unconstrained("auto_increment", ValueType::Boolean),
			RowShapeField::unconstrained("constraint", ValueType::Blob),
			RowShapeField::unconstrained("dictionary_id", ValueType::Uint8),
		])
	});
}

pub(crate) mod primitive_column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const INDEX: usize = 2;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("index", ValueType::Uint1),
		])
	});
}
