// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::id::SeriesId, key::series::SeriesKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	CatalogStore, Result,
	store::series::shape::series::{self, ID},
};

pub(crate) fn load_series(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SeriesKey::full_scan();

	let mut id_versions = Vec::new();
	{
		let stream = rx.range(range, RangeScope::All, 1024)?;
		for entry in stream {
			let multi = entry?;
			let id = SeriesId(series::SHAPE.get_u64(&multi.row, ID));
			id_versions.push((id, multi.version));
		}
	}

	for (id, version) in id_versions {
		if let Some(series) = CatalogStore::find_series(rx, id)? {
			catalog.set_series(id, version, Some(series));
		}
	}

	Ok(())
}
