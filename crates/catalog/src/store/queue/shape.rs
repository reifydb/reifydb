// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use once_cell::sync::Lazy;
use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
use reifydb_value::value::value_type::ValueType;

pub(crate) mod queue {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const PARTITIONS: usize = 3;
	pub(crate) const ORDERED_BY: usize = 4;
	pub(crate) const RETENTION_DONE: usize = 5;
	pub(crate) const RETRY_ATTEMPTS: usize = 6;
	pub(crate) const RETRY_BACKOFF: usize = 7;
	pub(crate) const UNDERLYING: usize = 8;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("namespace", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("partitions", ValueType::Uint2),
			RowShapeField::unconstrained("ordered_by", ValueType::Utf8),
			RowShapeField::unconstrained("retention_done", ValueType::Duration),
			RowShapeField::unconstrained("retry_attempts", ValueType::Uint4),
			RowShapeField::unconstrained("retry_backoff", ValueType::Duration),
			RowShapeField::unconstrained("underlying", ValueType::Uint1),
		])
	});
}

pub(crate) mod queue_namespace {
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
