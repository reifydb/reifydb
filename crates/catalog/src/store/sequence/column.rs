// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ColumnId, primitive::PrimitiveId},
	key::column_sequence::ColumnSequenceKey,
};
use reifydb_type::value::{Value, r#type::Type};

use super::generator::SequenceTransaction;
use crate::{
	CatalogStore,
	store::sequence::generator::{
		i8::GeneratorI8, i16::GeneratorI16, i32::GeneratorI32, i64::GeneratorI64, i128::GeneratorI128,
		u8::GeneratorU8, u16::GeneratorU16, u32::GeneratorU32, u64::GeneratorU64, u128::GeneratorU128,
	},
};

pub struct ColumnSequence {}

impl ColumnSequence {
	pub(crate) fn next_value(
		txn: &mut impl SequenceTransaction,
		source: impl Into<PrimitiveId>,
		column: ColumnId,
	) -> crate::Result<Value> {
		let column = CatalogStore::get_column(txn, column)?;
		let key = ColumnSequenceKey::encoded(source, column.id);

		Ok(match column.constraint.get_type() {
			Type::Int1 => Value::Int1(GeneratorI8::next(txn, &key, None)?),
			Type::Int2 => Value::Int2(GeneratorI16::next(txn, &key, None)?),
			Type::Int4 => Value::Int4(GeneratorI32::next(txn, &key, None)?),
			Type::Int8 => Value::Int8(GeneratorI64::next(txn, &key, None)?),
			Type::Int16 => Value::Int16(GeneratorI128::next(txn, &key, None)?),
			Type::Uint1 => Value::Uint1(GeneratorU8::next(txn, &key, None)?),
			Type::Uint2 => Value::Uint2(GeneratorU16::next(txn, &key, None)?),
			Type::Uint4 => Value::Uint4(GeneratorU32::next(txn, &key, None)?),
			Type::Uint8 => Value::Uint8(GeneratorU64::next(txn, &key, None)?),
			Type::Uint16 => Value::Uint16(GeneratorU128::next(txn, &key, None)?),
			_ => Value::Undefined,
		})
	}

	pub(crate) fn set_value(
		txn: &mut impl SequenceTransaction,
		source: impl Into<PrimitiveId>,
		column: ColumnId,
		value: Value,
	) -> crate::Result<()> {
		// let table = CatalogStore::get_table(txn, table)?;
		let column = CatalogStore::get_column(txn, column)?;

		if !column.auto_increment {
			// return_error!(can_not_alter_not_auto_increment(plan.
			// column, column.constraint.ty()));
			unimplemented!()
		}

		debug_assert!(value.get_type() == column.constraint.get_type());

		let key = ColumnSequenceKey::encoded(source, column.id);
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
