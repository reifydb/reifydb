// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	EncodableKey, UnderlyingCommandTransaction, TableId, TableRowSequenceKey
	,
};

use crate::{row::RowNumber, sequence::generator::u64::GeneratorU64};

pub struct TableRowSequence {}

impl TableRowSequence {
	pub fn next_row_number(
		txn: &mut impl UnderlyingCommandTransaction,
		table: TableId,
	) -> crate::Result<RowNumber> {
		GeneratorU64::next(
			txn,
			&TableRowSequenceKey {
				table,
			}
			.encode(),
			None,
		)
		.map(RowNumber)
	}
}
