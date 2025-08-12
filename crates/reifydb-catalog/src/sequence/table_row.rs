// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::row::RowId;
use crate::sequence::generator::u64::GeneratorU64;
use reifydb_core::interface::{
	ActiveCommandTransaction, EncodableKey, TableId, TableRowSequenceKey,
	Transaction,
};

pub struct TableRowSequence {}

impl TableRowSequence {
    pub fn next_row_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
		table: TableId,
    ) -> crate::Result<RowId> {
        GeneratorU64::next(txn, &TableRowSequenceKey { table }.encode()).map(RowId)
    }
}
