// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{CommandTransaction, EncodableKey, RowSequenceKey, SourceId, TableId};
use reifydb_type::RowNumber;

use crate::sequence::generator::u64::GeneratorU64;

pub struct RowSequence {}

impl RowSequence {
	pub fn next_row_number(txn: &mut impl CommandTransaction, table: TableId) -> crate::Result<RowNumber> {
		GeneratorU64::next(
			txn,
			&RowSequenceKey {
				source: SourceId::from(table),
			}
			.encode(),
			None,
		)
		.map(RowNumber)
	}
}
