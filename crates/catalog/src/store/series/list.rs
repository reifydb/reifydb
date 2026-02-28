// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SeriesId, series::SeriesDef},
	key::{Key, series::SeriesKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn list_series_all(rx: &mut Transaction<'_>) -> Result<Vec<SeriesDef>> {
		let mut result = Vec::new();

		let mut series_data: Vec<SeriesId> = Vec::new();
		{
			let mut stream = rx.range(SeriesKey::full_scan(), 1024)?;

			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key) {
					if let Key::Series(series_key) = key {
						series_data.push(series_key.series);
					}
				}
			}
		}

		for series_id in series_data {
			result.push(Self::get_series(rx, series_id)?);
		}

		Ok(result)
	}
}
