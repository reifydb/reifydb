// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	interface::catalog::{key::PrimaryKey, table::Table},
	sort::SortDirection,
	value::index::{encoded::EncodedIndexKey, shape::IndexShape},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use crate::Result;

pub fn encode_primary_key(
	pk_def: &PrimaryKey,
	row: &EncodedRow,
	table: &Table,
	shape: &RowShape,
) -> Result<EncodedIndexKey> {
	let types: Vec<ValueType> = pk_def.columns.iter().map(|c| c.constraint.get_type()).collect();
	let directions = vec![SortDirection::Asc; types.len()];
	let index_shape = IndexShape::new(&types, &directions)?;

	let mut index_key = index_shape.allocate_key();

	for (pk_idx, pk_column) in pk_def.columns.iter().enumerate() {
		let table_idx = table
			.columns
			.iter()
			.position(|c| c.id == pk_column.id)
			.expect("Primary key column not found in table");

		match pk_column.constraint.get_type() {
			ValueType::Boolean => {
				let val = shape.get_bool(row, table_idx);
				index_shape.set_bool(&mut index_key, pk_idx, val);
			}
			ValueType::Int1 => {
				let val = shape.get_i8(row, table_idx);
				index_shape.set_i8(&mut index_key, pk_idx, val);
			}
			ValueType::Int2 => {
				let val = shape.get_i16(row, table_idx);
				index_shape.set_i16(&mut index_key, pk_idx, val);
			}
			ValueType::Int4 => {
				let val = shape.get_i32(row, table_idx);
				index_shape.set_i32(&mut index_key, pk_idx, val);
			}
			ValueType::Int8 => {
				let val = shape.get_i64(row, table_idx);
				index_shape.set_i64(&mut index_key, pk_idx, val);
			}
			ValueType::Int16 => {
				let val = shape.get_i128(row, table_idx);
				index_shape.set_i128(&mut index_key, pk_idx, val);
			}
			ValueType::Uint1 => {
				let val = shape.get_u8(row, table_idx);
				index_shape.set_u8(&mut index_key, pk_idx, val);
			}
			ValueType::Uint2 => {
				let val = shape.get_u16(row, table_idx);
				index_shape.set_u16(&mut index_key, pk_idx, val);
			}
			ValueType::Uint4 => {
				let val = shape.get_u32(row, table_idx);
				index_shape.set_u32(&mut index_key, pk_idx, val);
			}
			ValueType::Uint8 => {
				let val = shape.get_u64(row, table_idx);
				index_shape.set_u64(&mut index_key, pk_idx, val);
			}
			ValueType::Uint16 => {
				let val = shape.get_u128(row, table_idx);
				index_shape.set_u128(&mut index_key, pk_idx, val);
			}
			ValueType::Float4 => {
				let val = shape.get_f32(row, table_idx);
				index_shape.set_f32(&mut index_key, pk_idx, val);
			}
			ValueType::Float8 => {
				let val = shape.get_f64(row, table_idx);
				index_shape.set_f64(&mut index_key, pk_idx, val);
			}
			ValueType::Utf8 => {
				panic!("UTF8 columns in primary keys not yet supported");
			}
			ValueType::Blob => {
				panic!("Blob columns cannot be used in primary keys");
			}
			ValueType::Date => {
				let val = shape.get_date(row, table_idx);
				index_shape.set_date(&mut index_key, pk_idx, val);
			}
			ValueType::Time => {
				let val = shape.get_time(row, table_idx);
				index_shape.set_time(&mut index_key, pk_idx, val);
			}
			ValueType::DateTime => {
				let val = shape.get_datetime(row, table_idx);
				index_shape.set_datetime(&mut index_key, pk_idx, val);
			}
			ValueType::Duration => {
				let val = shape.get_duration(row, table_idx);
				index_shape.set_duration(&mut index_key, pk_idx, val);
			}
			ValueType::Uuid4 => {
				let val = shape.get_uuid4(row, table_idx);
				index_shape.set_uuid4(&mut index_key, pk_idx, val);
			}
			ValueType::Uuid7 => {
				let val = shape.get_uuid7(row, table_idx);
				index_shape.set_uuid7(&mut index_key, pk_idx, val);
			}
			ValueType::IdentityId => {
				let val = shape.get_identity_id(row, table_idx);
				index_shape.set_identity_id(&mut index_key, pk_idx, val);
			}
			ValueType::Int => {
				panic!("Int columns in primary keys not yet supported");
			}
			ValueType::Uint => {
				panic!("Uint columns in primary keys not yet supported");
			}
			ValueType::Decimal => {
				panic!("Decimal columns in primary keys not yet supported");
			}
			ValueType::Option(_) => {
				index_shape.set_none(&mut index_key, pk_idx);
			}
			ValueType::DictionaryId => {
				panic!("DictionaryId columns cannot be used in primary keys");
			}
			ValueType::Any => {
				panic!("Any type cannot be used in primary keys");
			}
			ValueType::List(_) => {
				panic!("List type cannot be used in primary keys");
			}
			ValueType::Record(_) => {
				panic!("Record type cannot be used in primary keys");
			}
			ValueType::Tuple(_) => {
				panic!("Tuple type cannot be used in primary keys");
			}
		}
	}

	Ok(index_key)
}

pub fn get_primary_key(catalog: &Catalog, txn: &mut Transaction<'_>, table: &Table) -> Result<Option<PrimaryKey>> {
	if let Some(_pk_id) = catalog.get_table_pk_id(txn, table.id)? {
		catalog.find_primary_key(txn, table.id)
	} else {
		Ok(None)
	}
}
