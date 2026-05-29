// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::shape::{RowShape, RowShapeField};
use reifydb_type::value::r#type::Type;

pub(crate) mod column_snapshot {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const KIND: usize = 2;
	pub(crate) const SOURCE_ID: usize = 3;
	pub(crate) const BUCKET_START: usize = 4;
	pub(crate) const BUCKET_WIDTH: usize = 5;
	pub(crate) const SEQUENCE_COUNTER: usize = 6;
	pub(crate) const READ_VERSION: usize = 7;
	pub(crate) const ROW_COUNT: usize = 8;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("namespace", Type::Uint8),
			RowShapeField::unconstrained("kind", Type::Uint1),
			RowShapeField::unconstrained("source_id", Type::Uint8),
			RowShapeField::unconstrained("bucket_start", Type::Uint8),
			RowShapeField::unconstrained("bucket_width", Type::Uint8),
			RowShapeField::unconstrained("sequence_counter", Type::Uint8),
			RowShapeField::unconstrained("read_version", Type::Uint8),
			RowShapeField::unconstrained("row_count", Type::Uint8),
		])
	});
}

pub(crate) mod column_snapshot_link {
	use super::*;

	pub(crate) const ID: usize = 0;

	pub(crate) static SHAPE: Lazy<RowShape> =
		Lazy::new(|| RowShape::new(vec![RowShapeField::unconstrained("id", Type::Uint8)]));
}
