// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SourceId, source::SourceDef},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_source(rx: &mut Transaction<'_>, source: SourceId) -> Result<SourceDef> {
		CatalogStore::find_source(rx, source)?.ok_or_else(|| {
			Error(internal!(
				"Source with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				source
			))
		})
	}
}
