// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ActiveCommandTransaction, EncodableKey, TableId, TableRowSequenceKey,
	Transaction,
};

use crate::{row::RowId, sequence::generator::u64::GeneratorU64};

pub struct TableRowSequence {}

impl TableRowSequence {
	pub fn next_row_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
		table: TableId,
	) -> crate::Result<RowId> {
		GeneratorU64::next(
			txn,
			&TableRowSequenceKey {
				table,
			}
			.encode(),
		)
		.map(RowId)
	}
}
