// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{QueryTransaction, SequenceId},
};
use reifydb_type::internal;

use crate::{CatalogStore, store::sequence::Sequence};

impl CatalogStore {
	pub fn get_sequence(rx: &mut impl QueryTransaction, sequence_id: SequenceId) -> crate::Result<Sequence> {
		CatalogStore::find_sequence(rx, sequence_id)?.ok_or_else(|| {
			Error(internal!(
				"Sequence with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				sequence_id
			))
		})
	}
}
