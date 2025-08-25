// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, EncodableKey, ViewId, ViewRowSequenceKey,
};

use crate::{row::RowNumber, sequence::generator::u64::GeneratorU64};

pub struct ViewRowSequence {}

impl ViewRowSequence {
	pub fn next_row_number(
		txn: &mut impl CommandTransaction,
		view: ViewId,
	) -> crate::Result<RowNumber> {
		GeneratorU64::next(
			txn,
			&ViewRowSequenceKey {
				view,
			}
			.encode(),
			None,
		)
		.map(RowNumber)
	}
}
