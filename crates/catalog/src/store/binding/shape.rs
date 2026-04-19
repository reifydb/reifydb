// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod binding {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const PROCEDURE_ID: usize = 3;
	pub(crate) const PROTOCOL: usize = 4;
	pub(crate) const HTTP_METHOD: usize = 5;
	pub(crate) const HTTP_PATH: usize = 6;
	pub(crate) const RPC_NAME: usize = 7;
	pub(crate) const FORMAT: usize = 8;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("namespace", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("procedure_id", Type::Uint8),
			RowShapeField::unconstrained("protocol", Type::Utf8),
			RowShapeField::unconstrained("http_method", Type::Utf8),
			RowShapeField::unconstrained("http_path", Type::Utf8),
			RowShapeField::unconstrained("rpc_name", Type::Utf8),
			RowShapeField::unconstrained("format", Type::Utf8),
		])
	});
}

pub(crate) mod binding_namespace {
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
