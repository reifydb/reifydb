// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnId, ColumnSequenceKey, CommandTransaction, PrimitiveId};
use reifydb_type::{Type, Value};

use crate::{
	CatalogStore,
	store::sequence::generator::{
		i8::GeneratorI8, i16::GeneratorI16, i32::GeneratorI32, i64::GeneratorI64, i128::GeneratorI128,
		u8::GeneratorU8, u16::GeneratorU16, u32::GeneratorU32, u64::GeneratorU64, u128::GeneratorU128,
	},
};

pub struct ColumnSequence {}

impl ColumnSequence {
	pub async fn next_value(
		txn: &mut impl CommandTransaction,
		source: impl Into<PrimitiveId>,
		column: ColumnId,
	) -> crate::Result<Value> {
		let column = CatalogStore::get_column(txn, column).await?;
		let key = ColumnSequenceKey::encoded(source, column.id);

		Ok(match column.constraint.get_type() {
			Type::Int1 => Value::Int1(GeneratorI8::next(txn, &key, None).await?),
			Type::Int2 => Value::Int2(GeneratorI16::next(txn, &key, None).await?),
			Type::Int4 => Value::Int4(GeneratorI32::next(txn, &key, None).await?),
			Type::Int8 => Value::Int8(GeneratorI64::next(txn, &key, None).await?),
			Type::Int16 => Value::Int16(GeneratorI128::next(txn, &key, None).await?),
			Type::Uint1 => Value::Uint1(GeneratorU8::next(txn, &key, None).await?),
			Type::Uint2 => Value::Uint2(GeneratorU16::next(txn, &key, None).await?),
			Type::Uint4 => Value::Uint4(GeneratorU32::next(txn, &key, None).await?),
			Type::Uint8 => Value::Uint8(GeneratorU64::next(txn, &key, None).await?),
			Type::Uint16 => Value::Uint16(GeneratorU128::next(txn, &key, None).await?),
			_ => Value::Undefined,
		})
	}

	pub async fn set_value(
		txn: &mut impl CommandTransaction,
		source: impl Into<PrimitiveId>,
		column: ColumnId,
		value: Value,
	) -> crate::Result<()> {
		// let table = CatalogStore::get_table(txn, table)?;
		let column = CatalogStore::get_column(txn, column).await?;

		if !column.auto_increment {
			// return_error!(can_not_alter_not_auto_increment(plan.
			// column, column.constraint.ty()));
			unimplemented!()
		}

		debug_assert!(value.get_type() == column.constraint.get_type());

		let key = ColumnSequenceKey::encoded(source, column.id);
		match value {
			Value::Int1(v) => GeneratorI8::set(txn, &key, v).await,
			Value::Int2(v) => GeneratorI16::set(txn, &key, v).await,
			Value::Int4(v) => GeneratorI32::set(txn, &key, v).await,
			Value::Int8(v) => GeneratorI64::set(txn, &key, v).await,
			Value::Int16(v) => GeneratorI128::set(txn, &key, v).await,
			Value::Uint1(v) => GeneratorU8::set(txn, &key, v).await,
			Value::Uint2(v) => GeneratorU16::set(txn, &key, v).await,
			Value::Uint4(v) => GeneratorU32::set(txn, &key, v).await,
			Value::Uint8(v) => GeneratorU64::set(txn, &key, v).await,
			Value::Uint16(v) => GeneratorU128::set(txn, &key, v).await,
			_ => unreachable!(),
		}
	}
}
