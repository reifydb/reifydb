// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::QueryTransaction;
use system::ids::sequences::ALL;

use crate::{CatalogStore, store::sequence::Sequence, system};

impl CatalogStore {
	pub async fn list_sequences(rx: &mut impl QueryTransaction) -> crate::Result<Vec<Sequence>> {
		let mut result = Vec::with_capacity(ALL.len());

		for seq_id in ALL {
			result.push(CatalogStore::get_sequence(rx, seq_id).await?);
		}

		Ok(result)
	}
}
