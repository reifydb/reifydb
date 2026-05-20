// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SourceId, source::Source},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_source(rx: &mut Transaction<'_>, source: SourceId) -> Result<Source> {
		CatalogStore::find_source(rx, source)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"Source with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				source
			)))
		})
	}
}
