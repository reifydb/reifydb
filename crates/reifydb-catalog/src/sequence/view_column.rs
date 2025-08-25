// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Type, Value,
	interface::{
		CommandTransaction, EncodableKey, ViewColumnId,
		ViewColumnSequenceKey, ViewId,
	},
};

use crate::{
	CatalogStore,
	sequence::generator::{
		i8::GeneratorI8, i16::GeneratorI16, i32::GeneratorI32,
		i64::GeneratorI64, i128::GeneratorI128, u8::GeneratorU8,
		u16::GeneratorU16, u32::GeneratorU32, u64::GeneratorU64,
		u128::GeneratorU128,
	},
};

pub struct ViewColumnSequence {}

impl ViewColumnSequence {
	pub fn next_value(
		txn: &mut impl CommandTransaction,
		view: ViewId,
		column: ViewColumnId,
	) -> crate::Result<Value> {
		let column = CatalogStore::get_view_column(txn, column)?;
		let key = ViewColumnSequenceKey {
			view,
			column: column.id,
		}
		.encode();

		Ok(match column.ty {
			Type::Int1 => {
				Value::Int1(GeneratorI8::next(txn, &key, None)?)
			}
			Type::Int2 => Value::Int2(GeneratorI16::next(
				txn, &key, None,
			)?),
			Type::Int4 => Value::Int4(GeneratorI32::next(
				txn, &key, None,
			)?),
			Type::Int8 => Value::Int8(GeneratorI64::next(
				txn, &key, None,
			)?),
			Type::Int16 => Value::Int16(GeneratorI128::next(
				txn, &key, None,
			)?),
			Type::Uint1 => Value::Uint1(GeneratorU8::next(
				txn, &key, None,
			)?),
			Type::Uint2 => Value::Uint2(GeneratorU16::next(
				txn, &key, None,
			)?),
			Type::Uint4 => Value::Uint4(GeneratorU32::next(
				txn, &key, None,
			)?),
			Type::Uint8 => Value::Uint8(GeneratorU64::next(
				txn, &key, None,
			)?),
			Type::Uint16 => Value::Uint16(GeneratorU128::next(
				txn, &key, None,
			)?),
			_ => Value::Undefined,
		})
	}

	pub fn set_value(
		txn: &mut impl CommandTransaction,
		view: ViewId,
		column: ViewColumnId,
		value: Value,
	) -> crate::Result<()> {
		let view = CatalogStore::get_view(txn, view)?;

		let column = CatalogStore::get_view_column(txn, column)?;

		debug_assert!(value.get_type() == column.ty);

		let key = ViewColumnSequenceKey {
			view: view.id,
			column: column.id,
		}
		.encode();

		match value {
			Value::Int1(v) => GeneratorI8::set(txn, &key, v),
			Value::Int2(v) => GeneratorI16::set(txn, &key, v),
			Value::Int4(v) => GeneratorI32::set(txn, &key, v),
			Value::Int8(v) => GeneratorI64::set(txn, &key, v),
			Value::Int16(v) => GeneratorI128::set(txn, &key, v),
			Value::Uint1(v) => GeneratorU8::set(txn, &key, v),
			Value::Uint2(v) => GeneratorU16::set(txn, &key, v),
			Value::Uint4(v) => GeneratorU32::set(txn, &key, v),
			Value::Uint8(v) => GeneratorU64::set(txn, &key, v),
			Value::Uint16(v) => GeneratorU128::set(txn, &key, v),
			_ => unreachable!(),
		}
	}
}
