// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;
use system::ids::sequences::ALL;

use crate::{CatalogStore, Result, store::sequence::Sequence, system};

impl CatalogStore {
	pub(crate) fn list_sequences(rx: &mut Transaction<'_>) -> Result<Vec<Sequence>> {
		let mut result = Vec::with_capacity(ALL.len());

		for seq_id in ALL {
			result.push(CatalogStore::get_sequence(rx, seq_id)?);
		}

		Ok(result)
	}
}
