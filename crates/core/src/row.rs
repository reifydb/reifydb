// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::{
	row::EncodedRow,
	shape::{RowShape, RowShapeField},
};
use reifydb_value::value::{
	constraint::{Constraint, TypeConstraint},
	duration::Duration,
	row_number::RowNumber,
};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::column::Column;

#[derive(Debug, Clone)]
pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedRow,
	pub shape: RowShape,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ttl {
	pub duration: Duration,

	pub cleanup_mode: TtlCleanupMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowSettings {
	pub ttl: Option<Ttl>,

	pub persistent: bool,
}

impl RowSettings {
	pub fn is_persistent(&self) -> bool {
		self.persistent
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorSettings {
	pub ttl: Option<Ttl>,

	pub join: Option<JoinTtl>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TtlCleanupMode {
	Delete,

	Drop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinTtl {
	pub left: Option<Ttl>,

	pub right: Option<Ttl>,
}

pub fn row_shape_from_columns(value: &[Column]) -> RowShape {
	{
		let fields = value
			.iter()
			.map(|col| {
				let constraint = match col.constraint.constraint() {
					Some(Constraint::Dictionary(dict_id, id_type)) => {
						TypeConstraint::dictionary(*dict_id, id_type.clone())
					}
					_ => col.constraint.clone(),
				};
				RowShapeField::new(col.name.clone(), constraint)
			})
			.collect();
		RowShape::new(fields)
	}
}

#[cfg(test)]
mod tests {
	mod from_shape {
		// Tests removed as From<&RowShape> for the old layout type has been removed
		// RowShape is now the canonical layout descriptor
	}

	mod from_column {
		use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
		use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

		use crate::{
			interface::catalog::{
				column::{Column, ColumnIndex},
				id::ColumnId,
			},
			row::row_shape_from_columns,
		};

		fn make_column(id: u64, name: &str, ty: ValueType, index: u8) -> Column {
			Column {
				id: ColumnId(id),
				name: name.to_string(),
				constraint: TypeConstraint::unconstrained(ty),
				properties: vec![],
				index: ColumnIndex(index),
				auto_increment: false,
				dictionary_id: None,
			}
		}

		#[test]
		fn test_from_column_single_field() {
			let columns = vec![make_column(1, "id", ValueType::Int8, 0)];

			let shape = row_shape_from_columns(columns.as_slice());

			assert_eq!(shape.fields().len(), 1);
			assert_eq!(shape.fields()[0].name, "id");
			assert_eq!(shape.fields()[0].constraint.get_type(), ValueType::Int8);
		}

		#[test]
		fn test_from_column_multiple_fields() {
			let columns = vec![
				make_column(1, "a", ValueType::Int1, 0),
				make_column(2, "b", ValueType::Int2, 1),
				make_column(3, "c", ValueType::Int4, 2),
			];

			let shape = row_shape_from_columns(columns.as_slice());

			assert_eq!(shape.fields().len(), 3);
			assert_eq!(shape.fields()[0].name, "a");
			assert_eq!(shape.fields()[0].constraint.get_type(), ValueType::Int1);
			assert_eq!(shape.fields()[1].name, "b");
			assert_eq!(shape.fields()[1].constraint.get_type(), ValueType::Int2);
			assert_eq!(shape.fields()[2].name, "c");
			assert_eq!(shape.fields()[2].constraint.get_type(), ValueType::Int4);
		}

		#[test]
		fn test_from_column_preserves_field_order() {
			let columns = vec![
				make_column(1, "first", ValueType::Utf8, 0),
				make_column(2, "second", ValueType::Int4, 1),
				make_column(3, "third", ValueType::Boolean, 2),
			];

			let shape = row_shape_from_columns(columns.as_slice());

			assert_eq!(shape.fields()[0].name, "first");
			assert_eq!(shape.fields()[0].constraint.get_type(), ValueType::Utf8);
			assert_eq!(shape.fields()[1].name, "second");
			assert_eq!(shape.fields()[1].constraint.get_type(), ValueType::Int4);
			assert_eq!(shape.fields()[2].name, "third");
			assert_eq!(shape.fields()[2].constraint.get_type(), ValueType::Boolean);
		}

		#[test]
		fn test_from_column_equivalence_with_direct_construction() {
			let columns = vec![
				make_column(1, "f0", ValueType::Uint1, 0),
				make_column(2, "f1", ValueType::Uint2, 1),
				make_column(3, "f2", ValueType::Uint4, 2),
				make_column(4, "f3", ValueType::Uint8, 3),
				make_column(5, "f4", ValueType::Uint16, 4),
			];

			let shape_from_columns = row_shape_from_columns(columns.as_slice());
			let shape_direct = RowShape::new(vec![
				RowShapeField::unconstrained("f0", ValueType::Uint1),
				RowShapeField::unconstrained("f1", ValueType::Uint2),
				RowShapeField::unconstrained("f2", ValueType::Uint4),
				RowShapeField::unconstrained("f3", ValueType::Uint8),
				RowShapeField::unconstrained("f4", ValueType::Uint16),
			]);

			// Full equivalence check
			assert_eq!(shape_from_columns.fields().len(), shape_direct.fields().len());
			assert_eq!(shape_from_columns.fingerprint(), shape_direct.fingerprint());

			for (i, (from_columns, direct)) in
				shape_from_columns.fields().iter().zip(shape_direct.fields().iter()).enumerate()
			{
				assert_eq!(from_columns.name, direct.name, "name mismatch at field {}", i);
				assert_eq!(
					from_columns.constraint, direct.constraint,
					"constraint mismatch at field {}",
					i
				);
				assert_eq!(from_columns.offset, direct.offset, "offset mismatch at field {}", i);
				assert_eq!(from_columns.size, direct.size, "size mismatch at field {}", i);
				assert_eq!(from_columns.align, direct.align, "align mismatch at field {}", i);
			}
		}

		#[test]
		fn test_from_column_empty() {
			let columns: Vec<Column> = vec![];

			let shape = row_shape_from_columns(columns.as_slice());

			assert_eq!(shape.fields().len(), 0);
		}

		#[test]
		fn test_from_column_nine_fields() {
			let columns = vec![
				make_column(1, "f0", ValueType::Boolean, 0),
				make_column(2, "f1", ValueType::Int1, 1),
				make_column(3, "f2", ValueType::Int2, 2),
				make_column(4, "f3", ValueType::Int4, 3),
				make_column(5, "f4", ValueType::Int8, 4),
				make_column(6, "f5", ValueType::Uint1, 5),
				make_column(7, "f6", ValueType::Uint2, 6),
				make_column(8, "f7", ValueType::Uint4, 7),
				make_column(9, "f8", ValueType::Uint8, 8),
			];

			let shape = row_shape_from_columns(columns.as_slice());

			assert_eq!(shape.fields().len(), 9);
			for (i, field) in shape.fields().iter().enumerate() {
				assert_eq!(field.name, format!("f{}", i));
			}
		}
	}
}
