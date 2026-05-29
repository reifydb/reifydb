// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::shape::{RowShape, RowShapeField};
use reifydb_value::value::value_type::ValueType;

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
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("namespace", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("capacity", ValueType::Uint8),
			RowShapeField::unconstrained("primary_key", ValueType::Uint8),
			RowShapeField::unconstrained("partition_by", ValueType::Utf8),
			RowShapeField::unconstrained("underlying", ValueType::Uint1),
		])
	});
}

pub(crate) mod ringbuffer_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
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
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("capacity", ValueType::Uint8),
			RowShapeField::unconstrained("head", ValueType::Uint8),
			RowShapeField::unconstrained("tail", ValueType::Uint8),
			RowShapeField::unconstrained("count", ValueType::Uint8),
		])
	});
}
