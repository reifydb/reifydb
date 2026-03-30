// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SeriesId, series::Series},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_series(rx: &mut Transaction<'_>, series: SeriesId) -> Result<Series> {
		Self::find_series(rx, series)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"Series with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				series
			)))
		})
	}
}
