// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sequence::generator::u64::GeneratorU64;
use reifydb_core::Value;
use reifydb_core::interface::{
    ActiveWriteTransaction, ColumnId, EncodableKey, TableColumnSequenceKey, TableId,
    UnversionedTransaction, VersionedTransaction,
};

pub struct ColumnSequence {}

impl ColumnSequence {
    pub fn next_value<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        table: TableId,
        column: ColumnId,
    ) -> crate::Result<Value> {
        Ok(Value::Uint8(GeneratorU64::next(
            atx,
            &TableColumnSequenceKey { table, column }.encode(),
        )?))
    }
}
