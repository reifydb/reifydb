// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::SeriesId,
	key::{
		namespace_series::NamespaceSeriesKey,
		series::{SeriesKey, SeriesMetadataKey},
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::shape::drop::drop_shape_metadata};

impl CatalogStore {
	pub(crate) fn drop_series(txn: &mut AdminTransaction, series: SeriesId) -> Result<()> {
		let pk_id = if let Some(series_def) = Self::find_series(&mut Transaction::Admin(&mut *txn), series)? {
			txn.remove(&NamespaceSeriesKey::encoded(series_def.namespace, series))?;
			series_def.primary_key.as_ref().map(|pk| pk.id)
		} else {
			None
		};

		drop_shape_metadata(txn, series.into(), pk_id)?;

		txn.remove(&SeriesMetadataKey::encoded(series))?;

		txn.remove(&SeriesKey::encoded(series))?;

		Ok(())
	}
}
