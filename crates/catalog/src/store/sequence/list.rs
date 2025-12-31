// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_transaction::IntoStandardTransaction;
use system::ids::sequences::ALL;

use crate::{CatalogStore, store::sequence::Sequence, system};

impl CatalogStore {
	pub async fn list_sequences(rx: &mut impl IntoStandardTransaction) -> crate::Result<Vec<Sequence>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::with_capacity(ALL.len());

		for seq_id in ALL {
			result.push(CatalogStore::get_sequence(&mut txn, seq_id).await?);
		}

		Ok(result)
	}
}
