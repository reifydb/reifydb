// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub(crate) mod procedure {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;

	pub(crate) const VARIANT: usize = 3;
	pub(crate) const BODY: usize = 4;

	pub(crate) const TRIGGER_KIND: usize = 5;

	pub(crate) const TRIGGER_VARIANT_SUMTYPE: usize = 6;

	pub(crate) const TRIGGER_VARIANT_INDEX: usize = 7;

	pub(crate) const RETURN_TYPE: usize = 8;

	pub(crate) const VARIANT_RQL: u8 = 0;
	pub(crate) const VARIANT_TEST: u8 = 1;

	pub(crate) const TRIGGER_CALL: u8 = 0;
	pub(crate) const TRIGGER_EVENT: u8 = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("namespace", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("variant", ValueType::Uint1),
			RowShapeField::unconstrained("body", ValueType::Utf8),
			RowShapeField::unconstrained("trigger_kind", ValueType::Uint1),
			RowShapeField::unconstrained("trigger_variant_sumtype", ValueType::Uint8),
			RowShapeField::unconstrained("trigger_variant_index", ValueType::Uint2),
			RowShapeField::unconstrained("return_type", ValueType::Utf8),
		])
	});
}

pub(crate) mod namespace_procedure {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
		])
	});
}

pub(crate) mod procedure_param {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const PROCEDURE_ID: usize = 0;
	pub(crate) const INDEX: usize = 1;
	pub(crate) const NAME: usize = 2;

	pub(crate) const TYPE_CONSTRAINT: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("procedure_id", ValueType::Uint8),
			RowShapeField::unconstrained("index", ValueType::Uint2),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("type_constraint", ValueType::Utf8),
		])
	});
}
