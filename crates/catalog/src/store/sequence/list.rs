// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::ValueError;
use system::ids::sequences::ALL;

use crate::{CatalogStore, store::sequence::Sequence, system};

impl CatalogStore {
	pub(crate) fn list_sequences(rx: &mut Transaction<'_>) -> Result<Vec<Sequence>, ValueError> {
		let mut result = Vec::with_capacity(ALL.len());

		for (seq_id, _) in ALL {
			result.push(CatalogStore::get_sequence(rx, seq_id)?);
		}

		Ok(result)
	}
}
