// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::standard::IntoStandardTransaction;
use system::ids::sequences::ALL;

use crate::{CatalogStore, store::sequence::Sequence, system};

impl CatalogStore {
	pub(crate) fn list_sequences(rx: &mut impl IntoStandardTransaction) -> crate::Result<Vec<Sequence>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::with_capacity(ALL.len());

		for seq_id in ALL {
			result.push(CatalogStore::get_sequence(&mut txn, seq_id)?);
		}

		Ok(result)
	}
}
