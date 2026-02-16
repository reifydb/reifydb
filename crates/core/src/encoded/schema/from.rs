//  SPDX-License-Identifier: AGPL-3.0-or-later
//  Copyright (c) 2025 ReifyDB

use reifydb_type::value::constraint::TypeConstraint;

use crate::{
	encoded::schema::{Schema, SchemaField},
	interface::catalog::{column::ColumnDef, subscription::SubscriptionColumnDef},
};

impl From<&Vec<ColumnDef>> for Schema {
	fn from(value: &Vec<ColumnDef>) -> Self {
		Schema::from(value.as_slice())
	}
}
impl From<&[ColumnDef]> for Schema {
	fn from(value: &[ColumnDef]) -> Self {
		let fields =
			value.iter().map(|col| SchemaField::new(col.name.clone(), col.constraint.clone())).collect();
		Schema::new(fields)
	}
}

impl From<&Vec<SubscriptionColumnDef>> for Schema {
	fn from(value: &Vec<SubscriptionColumnDef>) -> Self {
		Schema::from(value.as_slice())
	}
}
impl From<&[SubscriptionColumnDef]> for Schema {
	fn from(value: &[SubscriptionColumnDef]) -> Self {
		let fields = value
			.iter()
			.map(|col| SchemaField::new(col.name.clone(), TypeConstraint::unconstrained(col.ty.clone())))
			.collect();
		Schema::new(fields)
	}
}

#[cfg(test)]
mod tests {
	mod from_schema {
		// Tests removed as From<&Schema> for EncodedValuesLayout has been removed
		// EncodedValuesLayout is being phased out in favor of Schema
	}

	mod from_column_def {
		use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

		use crate::{
			encoded::schema::{Schema, SchemaField},
			interface::catalog::{
				column::{ColumnDef, ColumnIndex},
				id::ColumnId,
			},
		};

		fn make_column_def(id: u64, name: &str, ty: Type, index: u8) -> ColumnDef {
			ColumnDef {
				id: ColumnId(id),
				name: name.to_string(),
				constraint: TypeConstraint::unconstrained(ty),
				policies: vec![],
				index: ColumnIndex(index),
				auto_increment: false,
				dictionary_id: None,
			}
		}

		#[test]
		fn test_from_column_def_single_field() {
			let columns = vec![make_column_def(1, "id", Type::Int8, 0)];

			let schema = Schema::from(columns.as_slice());

			assert_eq!(schema.fields().len(), 1);
			assert_eq!(schema.fields()[0].name, "id");
			assert_eq!(schema.fields()[0].constraint.get_type(), Type::Int8);
		}

		#[test]
		fn test_from_column_def_multiple_fields() {
			let columns = vec![
				make_column_def(1, "a", Type::Int1, 0),
				make_column_def(2, "b", Type::Int2, 1),
				make_column_def(3, "c", Type::Int4, 2),
			];

			let schema = Schema::from(columns.as_slice());

			assert_eq!(schema.fields().len(), 3);
			assert_eq!(schema.fields()[0].name, "a");
			assert_eq!(schema.fields()[0].constraint.get_type(), Type::Int1);
			assert_eq!(schema.fields()[1].name, "b");
			assert_eq!(schema.fields()[1].constraint.get_type(), Type::Int2);
			assert_eq!(schema.fields()[2].name, "c");
			assert_eq!(schema.fields()[2].constraint.get_type(), Type::Int4);
		}

		#[test]
		fn test_from_column_def_preserves_field_order() {
			let columns = vec![
				make_column_def(1, "first", Type::Utf8, 0),
				make_column_def(2, "second", Type::Int4, 1),
				make_column_def(3, "third", Type::Boolean, 2),
			];

			let schema = Schema::from(columns.as_slice());

			assert_eq!(schema.fields()[0].name, "first");
			assert_eq!(schema.fields()[0].constraint.get_type(), Type::Utf8);
			assert_eq!(schema.fields()[1].name, "second");
			assert_eq!(schema.fields()[1].constraint.get_type(), Type::Int4);
			assert_eq!(schema.fields()[2].name, "third");
			assert_eq!(schema.fields()[2].constraint.get_type(), Type::Boolean);
		}

		#[test]
		fn test_from_column_def_equivalence_with_direct_construction() {
			let columns = vec![
				make_column_def(1, "f0", Type::Uint1, 0),
				make_column_def(2, "f1", Type::Uint2, 1),
				make_column_def(3, "f2", Type::Uint4, 2),
				make_column_def(4, "f3", Type::Uint8, 3),
				make_column_def(5, "f4", Type::Uint16, 4),
			];

			let schema_from_columns = Schema::from(columns.as_slice());
			let schema_direct = Schema::new(vec![
				SchemaField::unconstrained("f0", Type::Uint1),
				SchemaField::unconstrained("f1", Type::Uint2),
				SchemaField::unconstrained("f2", Type::Uint4),
				SchemaField::unconstrained("f3", Type::Uint8),
				SchemaField::unconstrained("f4", Type::Uint16),
			]);

			// Full equivalence check
			assert_eq!(schema_from_columns.fields().len(), schema_direct.fields().len());
			assert_eq!(schema_from_columns.fingerprint(), schema_direct.fingerprint());

			for (i, (from_columns, direct)) in
				schema_from_columns.fields().iter().zip(schema_direct.fields().iter()).enumerate()
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
		fn test_from_column_def_empty() {
			let columns: Vec<ColumnDef> = vec![];

			let schema = Schema::from(columns.as_slice());

			assert_eq!(schema.fields().len(), 0);
		}

		#[test]
		fn test_from_column_def_nine_fields() {
			let columns = vec![
				make_column_def(1, "f0", Type::Boolean, 0),
				make_column_def(2, "f1", Type::Int1, 1),
				make_column_def(3, "f2", Type::Int2, 2),
				make_column_def(4, "f3", Type::Int4, 3),
				make_column_def(5, "f4", Type::Int8, 4),
				make_column_def(6, "f5", Type::Uint1, 5),
				make_column_def(7, "f6", Type::Uint2, 6),
				make_column_def(8, "f7", Type::Uint4, 7),
				make_column_def(9, "f8", Type::Uint8, 8),
			];

			let schema = Schema::from(columns.as_slice());

			assert_eq!(schema.fields().len(), 9);
			for (i, field) in schema.fields().iter().enumerate() {
				assert_eq!(field.name, format!("f{}", i));
			}
		}
	}
}
