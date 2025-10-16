// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	SortDirection,
	interface::{PrimaryKeyDef, QueryTransaction, TableDef},
	value::{
		encoded::{EncodedValues, EncodedValuesLayout},
		index::{EncodedIndexKey, EncodedIndexLayout},
	},
};
use reifydb_type::Type;

/// Extract primary key values from a encoded and encode them as an index key
pub fn encode_primary_key(
	pk_def: &PrimaryKeyDef,
	row: &EncodedValues,
	table: &TableDef,
	layout: &EncodedValuesLayout,
) -> crate::Result<EncodedIndexKey> {
	// Create index layout for PK columns
	let types: Vec<Type> = pk_def.columns.iter().map(|c| c.constraint.get_type()).collect();
	let directions = vec![SortDirection::Asc; types.len()];
	let index_layout = EncodedIndexLayout::new(&types, &directions)?;

	let mut index_key = index_layout.allocate_key();

	// Extract values from encoded for each PK column
	for (pk_idx, pk_column) in pk_def.columns.iter().enumerate() {
		// Find column index in table
		let table_idx = table
			.columns
			.iter()
			.position(|c| c.id == pk_column.id)
			.expect("Primary key column not found in table");

		// Check if value is defined
		// Note: EncodedRowLayout doesn't have is_defined for individual
		// fields, so we'll check by trying to get the value and
		// seeing if it's undefined For now, we'll assume all values
		// are defined

		// Copy value based on type
		match pk_column.constraint.get_type() {
			Type::Boolean => {
				let val = layout.get_bool(row, table_idx);
				index_layout.set_bool(&mut index_key, pk_idx, val);
			}
			Type::Int1 => {
				let val = layout.get_i8(row, table_idx);
				index_layout.set_i8(&mut index_key, pk_idx, val);
			}
			Type::Int2 => {
				let val = layout.get_i16(row, table_idx);
				index_layout.set_i16(&mut index_key, pk_idx, val);
			}
			Type::Int4 => {
				let val = layout.get_i32(row, table_idx);
				index_layout.set_i32(&mut index_key, pk_idx, val);
			}
			Type::Int8 => {
				let val = layout.get_i64(row, table_idx);
				index_layout.set_i64(&mut index_key, pk_idx, val);
			}
			Type::Int16 => {
				let val = layout.get_i128(row, table_idx);
				index_layout.set_i128(&mut index_key, pk_idx, val);
			}
			Type::Uint1 => {
				let val = layout.get_u8(row, table_idx);
				index_layout.set_u8(&mut index_key, pk_idx, val);
			}
			Type::Uint2 => {
				let val = layout.get_u16(row, table_idx);
				index_layout.set_u16(&mut index_key, pk_idx, val);
			}
			Type::Uint4 => {
				let val = layout.get_u32(row, table_idx);
				index_layout.set_u32(&mut index_key, pk_idx, val);
			}
			Type::Uint8 => {
				let val = layout.get_u64(row, table_idx);
				index_layout.set_u64(&mut index_key, pk_idx, val);
			}
			Type::Uint16 => {
				let val = layout.get_u128(row, table_idx);
				index_layout.set_u128(&mut index_key, pk_idx, val);
			}
			Type::Float4 => {
				let val = layout.get_f32(row, table_idx);
				index_layout.set_f32(&mut index_key, pk_idx, val);
			}
			Type::Float8 => {
				let val = layout.get_f64(row, table_idx);
				index_layout.set_f64(&mut index_key, pk_idx, val);
			}
			Type::Utf8 => {
				// UTF8 strings can't be used in indexes
				// currently This would require implementing
				// variable-length encoding
				panic!("UTF8 columns in primary keys not yet supported");
			}
			Type::Blob => {
				// Blobs can't be used in indexes
				panic!("Blob columns cannot be used in primary keys");
			}
			Type::Date => {
				let val = layout.get_date(row, table_idx);
				index_layout.set_date(&mut index_key, pk_idx, val);
			}
			Type::Time => {
				let val = layout.get_time(row, table_idx);
				index_layout.set_time(&mut index_key, pk_idx, val);
			}
			Type::DateTime => {
				let val = layout.get_datetime(row, table_idx);
				index_layout.set_datetime(&mut index_key, pk_idx, val);
			}
			Type::Duration => {
				let val = layout.get_duration(row, table_idx);
				index_layout.set_duration(&mut index_key, pk_idx, val);
			}
			Type::Uuid4 => {
				let val = layout.get_uuid4(row, table_idx);
				index_layout.set_uuid4(&mut index_key, pk_idx, val);
			}
			Type::Uuid7 => {
				let val = layout.get_uuid7(row, table_idx);
				index_layout.set_uuid7(&mut index_key, pk_idx, val);
			}
			Type::IdentityId => {
				let val = layout.get_identity_id(row, table_idx);
				index_layout.set_identity_id(&mut index_key, pk_idx, val);
			}
			Type::RowNumber => {
				// RowNumber shouldn't be in a primary key
				panic!("RowNumber columns cannot be used in primary keys");
			}
			Type::Int => {
				// Int columns in primary keys not yet
				// supported
				panic!("Int columns in primary keys not yet supported");
			}
			Type::Uint => {
				// Uint columns in primary keys not yet
				// supported
				panic!("Uint columns in primary keys not yet supported");
			}
			Type::Decimal {
				..
			} => {
				// Decimal columns in primary keys not yet
				// supported
				panic!("Decimal columns in primary keys not yet supported");
			}
			Type::Undefined => {
				// Undefined values in primary key will be
				// handled later with constraints
				index_layout.set_undefined(&mut index_key, pk_idx);
			}
			Type::Any => {
				panic!("Any type cannot be used in primary keys");
			}
		}
	}

	Ok(index_key)
}

/// Helper to load the primary key definition if the table has one
pub fn get_primary_key(txn: &mut impl QueryTransaction, table: &TableDef) -> crate::Result<Option<PrimaryKeyDef>> {
	if let Some(_pk_id) = CatalogStore::get_table_pk_id(txn, table.id)? {
		CatalogStore::find_primary_key(txn, table.id)
	} else {
		Ok(None)
	}
}
