// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;
use system::ids::sequences::ALL;

use crate::{CatalogStore, sequence::Sequence, system};

impl CatalogStore {
	pub fn list_sequences(
		rx: &mut impl QueryTransaction,
	) -> crate::Result<Vec<Sequence>> {
		let mut result = Vec::with_capacity(ALL.len());

		for seq_id in ALL {
			result.push(CatalogStore::get_sequence(rx, seq_id)?);
		}

		Ok(result)
	}
}
