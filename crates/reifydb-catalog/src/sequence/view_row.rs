// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ActiveCommandTransaction, EncodableKey, Transaction, ViewId,
	ViewRowSequenceKey,
};

use crate::{row::RowId, sequence::generator::u64::GeneratorU64};

pub struct ViewRowSequence {}

impl ViewRowSequence {
	pub fn next_row_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
		view: ViewId,
	) -> crate::Result<RowId> {
		GeneratorU64::next(
			txn,
			&ViewRowSequenceKey {
				view,
			}
			.encode(),
			None,
		)
		.map(RowId)
	}
}
