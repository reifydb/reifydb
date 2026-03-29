// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::shape::{RowShape, RowShapeField};
use reifydb_type::value::r#type::Type;

pub(crate) mod handler {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const ON_SUMTYPE_ID: usize = 3;
	pub(crate) const ON_VARIANT_TAG: usize = 4;
	pub(crate) const BODY_SOURCE: usize = 5;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("namespace", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("on_sumtype_id", Type::Uint8),
			RowShapeField::unconstrained("on_variant_tag", Type::Uint1),
			RowShapeField::unconstrained("body_source", Type::Utf8),
		])
	});
}

pub(crate) mod handler_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
		])
	});
}
