// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::{
	CatalogStore,
	sequence::{Sequence, system::ALL_SYSTEM_SEQUENCE_IDS},
};

impl CatalogStore {
	pub fn list_sequences(
		rx: &mut impl QueryTransaction,
	) -> crate::Result<Vec<Sequence>> {
		let mut result =
			Vec::with_capacity(ALL_SYSTEM_SEQUENCE_IDS.len());

		for seq_id in ALL_SYSTEM_SEQUENCE_IDS {
			result.push(CatalogStore::get_sequence(rx, seq_id)?);
		}

		Ok(result)
	}
}
