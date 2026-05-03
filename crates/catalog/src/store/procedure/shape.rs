// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Row shape for the persistent procedure record.
/// Covers both `Procedure::Rql` and `Procedure::Test` - discriminated by `VARIANT`.
/// Native/Ffi/Wasm variants are ephemeral and never written to storage.
pub(crate) mod procedure {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

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
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("namespace", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("variant", Type::Uint1),
			RowShapeField::unconstrained("body", Type::Utf8),
			RowShapeField::unconstrained("trigger_kind", Type::Uint1),
			RowShapeField::unconstrained("trigger_variant_sumtype", Type::Uint8),
			RowShapeField::unconstrained("trigger_variant_index", Type::Uint2),
			RowShapeField::unconstrained("return_type", Type::Utf8),
		])
	});
}

pub(crate) mod namespace_procedure {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
		])
	});
}

pub(crate) mod procedure_param {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const PROCEDURE_ID: usize = 0;
	pub(crate) const INDEX: usize = 1;
	pub(crate) const NAME: usize = 2;

	pub(crate) const TYPE_CONSTRAINT: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("procedure_id", Type::Uint8),
			RowShapeField::unconstrained("index", Type::Uint2),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("type_constraint", Type::Utf8),
		])
	});
}
