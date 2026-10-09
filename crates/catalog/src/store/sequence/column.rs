// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::num::NonZeroU64;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	interface::catalog::{id::ColumnId, object::ObjectId},
	key::column::ColumnSequenceKey,
	value::column::factory::{
		default_typed, int1, int2, int4, int8, int16, none_typed, uint1, uint2, uint4, uint8, uint16,
	},
};
use reifydb_value::{
	reifydb_assertions,
	value::{Value, value_type::ValueType},
};

use super::generator::SequenceTransaction;
use crate::{
	CatalogStore, Result,
	store::sequence::generator::{
		i8::GeneratorI8, i16::GeneratorI16, i32::GeneratorI32, i64::GeneratorI64, i128::GeneratorI128,
		u8::GeneratorU8, u16::GeneratorU16, u32::GeneratorU32, u64::GeneratorU64, u128::GeneratorU128,
	},
};

pub struct ColumnSequence {}

impl ColumnSequence {
	pub(crate) fn next_values(
		txn: &mut impl SequenceTransaction,
		object: impl Into<ObjectId>,
		column: ColumnId,
		count: u64,
	) -> Result<(FieldRef, ArrayRef)> {
		let column = CatalogStore::get_column(&mut txn.as_transaction(), column)?;
		let key = ColumnSequenceKey::new(object, column.id);
		let name = column.name.as_str();
		let ty = column.constraint.get_type();
		let Some(count) = NonZeroU64::new(count) else {
			return Ok(default_typed(name, ty, 0));
		};

		Ok(match ty {
			ValueType::Int1 => int1(name, GeneratorI8::next_run(txn, &key, count)?),
			ValueType::Int2 => int2(name, GeneratorI16::next_run(txn, &key, count)?),
			ValueType::Int4 => int4(name, GeneratorI32::next_run(txn, &key, count)?),
			ValueType::Int8 => int8(name, GeneratorI64::next_run(txn, &key, count)?),
			ValueType::Int16 => int16(name, GeneratorI128::next_run(txn, &key, count)?),
			ValueType::Uint1 => uint1(name, GeneratorU8::next_run(txn, &key, count)?),
			ValueType::Uint2 => uint2(name, GeneratorU16::next_run(txn, &key, count)?),
			ValueType::Uint4 => uint4(name, GeneratorU32::next_run(txn, &key, count)?),
			ValueType::Uint8 => uint8(name, GeneratorU64::next_run(txn, &key, count)?),
			ValueType::Uint16 => uint16(name, GeneratorU128::next_run(txn, &key, count)?),
			_ => none_typed(name, ty, count.get() as usize),
		})
	}

	pub(crate) fn set_value(
		txn: &mut impl SequenceTransaction,
		object: impl Into<ObjectId>,
		column: ColumnId,
		value: Value,
	) -> Result<()> {
		let column = CatalogStore::get_column(&mut txn.as_transaction(), column)?;

		if !column.auto_increment {
			unimplemented!()
		}

		reifydb_assertions! {
			assert!(value.get_type() == column.constraint.get_type());
		}

		let key = ColumnSequenceKey::new(object, column.id);
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
