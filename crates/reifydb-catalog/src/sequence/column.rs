// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::sequence::generator::i8::GeneratorI8;
use crate::sequence::generator::i16::GeneratorI16;
use crate::sequence::generator::i32::GeneratorI32;
use crate::sequence::generator::i64::GeneratorI64;
use crate::sequence::generator::i128::GeneratorI128;
use crate::sequence::generator::u8::GeneratorU8;
use crate::sequence::generator::u16::GeneratorU16;
use crate::sequence::generator::u32::GeneratorU32;
use crate::sequence::generator::u64::GeneratorU64;
use crate::sequence::generator::u128::GeneratorU128;
use reifydb_core::interface::{
    ActiveCommandTransaction, ColumnId, EncodableKey, TableColumnSequenceKey, TableId,
    Transaction,
};
use reifydb_core::{Type, Value};

pub struct ColumnSequence {}

impl ColumnSequence {
    pub fn next_value<T: Transaction>(
        txn: &mut ActiveCommandTransaction<T>,
        table: TableId,
        column: ColumnId,
    ) -> crate::Result<Value> {
        if let Some(column) = Catalog::get_column(txn, column)? {
            let key = TableColumnSequenceKey { table, column: column.id }.encode();

            Ok(match column.ty {
                Type::Int1 => Value::Int1(GeneratorI8::next(txn, &key)?),
                Type::Int2 => Value::Int2(GeneratorI16::next(txn, &key)?),
                Type::Int4 => Value::Int4(GeneratorI32::next(txn, &key)?),
                Type::Int8 => Value::Int8(GeneratorI64::next(txn, &key)?),
                Type::Int16 => Value::Int16(GeneratorI128::next(txn, &key)?),
                Type::Uint1 => Value::Uint1(GeneratorU8::next(txn, &key)?),
                Type::Uint2 => Value::Uint2(GeneratorU16::next(txn, &key)?),
                Type::Uint4 => Value::Uint4(GeneratorU32::next(txn, &key)?),
                Type::Uint8 => Value::Uint8(GeneratorU64::next(txn, &key)?),
                Type::Uint16 => Value::Uint16(GeneratorU128::next(txn, &key)?),
                _ => Value::Undefined,
            })
        } else {
            Ok(Value::Undefined)
        }
    }

    pub fn set_value<T: Transaction>(
        txn: &mut ActiveCommandTransaction<T>,
        table: TableId,
        column: ColumnId,
        value: Value,
    ) -> crate::Result<()> {
        let Some(table) = Catalog::get_table(txn, table)? else {
            // return_error!(table_not_found(plan.table.clone(), &schema.name, &plan.table.as_ref(),));
            unimplemented!()
        };

        let Some(column) = Catalog::get_column(txn, column)? else {
            // return_error!(column_not_found(plan.column.clone()));
            unimplemented!()
        };

        if !column.auto_increment {
            // return_error!(can_not_alter_not_auto_increment(plan.column, column.ty));
            unimplemented!()
        }

        debug_assert!(value.get_type() == column.ty);

        let key = TableColumnSequenceKey { table: table.id, column: column.id }.encode();
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
