// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod shape_header {
	use reifydb_codec::row::{pod::EncodedPodRow, shape::RowFamily};
	use reifydb_core::return_internal_error;

	use crate::Result;

	pub(crate) fn encode(family: RowFamily, field_count: u16) -> EncodedPodRow {
		let mut body = [0u8; 3];
		body[0] = family as u8;
		body[1..].copy_from_slice(&field_count.to_be_bytes());
		EncodedPodRow::new(&body)
	}

	pub(crate) fn decode(row: &EncodedPodRow) -> Result<(RowFamily, u16)> {
		let Ok(bytes) = <[u8; 3]>::try_from(row.body()) else {
			return_internal_error!(
				"Row-shape header is {} bytes wide, expected 3. This indicates a corrupt shape header.",
				row.len()
			)
		};
		let Some(family) = RowFamily::from_u8(bytes[0]) else {
			return_internal_error!(
				"Row-shape header carries unknown family tag {}. This indicates a corrupt shape header.",
				bytes[0]
			)
		};
		Ok((family, u16::from_be_bytes([bytes[1], bytes[2]])))
	}
}

pub(crate) mod shape_field {
	use once_cell::sync::Lazy;
	use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const NAME: usize = 0;

	pub(crate) const TYPE: usize = 1;

	pub(crate) const CONSTRAINT_TYPE: usize = 2;

	pub(crate) const CONSTRAINT_P1: usize = 3;

	pub(crate) const CONSTRAINT_P2: usize = 4;

	pub(crate) const OFFSET: usize = 5;

	pub(crate) const SIZE: usize = 6;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(
			RowFamily::Catalog,
			vec![
				RowShapeField::unconstrained("name", ValueType::Utf8),
				RowShapeField::unconstrained("base_type", ValueType::Uint1),
				RowShapeField::unconstrained("constraint_type", ValueType::Uint1),
				RowShapeField::unconstrained("constraint_p1", ValueType::Uint4),
				RowShapeField::unconstrained("constraint_p2", ValueType::Uint4),
				RowShapeField::unconstrained("offset", ValueType::Uint4),
				RowShapeField::unconstrained("size", ValueType::Uint4),
			],
		)
	});
}
