// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::shape::{RowShape, RowShapeField};
use reifydb_type::value::r#type::Type;

pub(crate) mod ringbuffer {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const CAPACITY: usize = 3;
	pub(crate) const PRIMARY_KEY: usize = 4;
	pub(crate) const PARTITION_BY: usize = 5;
	pub(crate) const UNDERLYING: usize = 6;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("namespace", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("capacity", Type::Uint8),
			RowShapeField::unconstrained("primary_key", Type::Uint8),
			RowShapeField::unconstrained("partition_by", Type::Utf8),
			RowShapeField::unconstrained("underlying", Type::Uint1),
		])
	});
}

pub(crate) mod ringbuffer_namespace {
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

pub(crate) mod ringbuffer_metadata {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const CAPACITY: usize = 1;
	pub(crate) const HEAD: usize = 2;
	pub(crate) const TAIL: usize = 3;
	pub(crate) const COUNT: usize = 4;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("capacity", Type::Uint8),
			RowShapeField::unconstrained("head", Type::Uint8),
			RowShapeField::unconstrained("tail", Type::Uint8),
			RowShapeField::unconstrained("count", Type::Uint8),
		])
	});
}
