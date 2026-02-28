// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SeriesId, series::SeriesDef},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_series(rx: &mut Transaction<'_>, series: SeriesId) -> Result<SeriesDef> {
		Self::find_series(rx, series)?.ok_or_else(|| {
			Error(internal!(
				"Series with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				series
			))
		})
	}
}
