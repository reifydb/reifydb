// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::id::SequenceId, internal};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, store::sequence::Sequence};

impl CatalogStore {
	pub(crate) fn get_sequence(
		rx: &mut impl IntoStandardTransaction,
		sequence_id: SequenceId,
	) -> crate::Result<Sequence> {
		CatalogStore::find_sequence(rx, sequence_id)?.ok_or_else(|| {
			Error(internal!(
				"Sequence with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sequence_id
			))
		})
	}
}
