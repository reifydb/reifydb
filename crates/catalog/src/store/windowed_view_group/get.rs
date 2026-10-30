// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::WindowedViewGroupId, windowed_view_group::WindowedViewGroup},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_windowed_view_group(
		rx: &mut Transaction<'_>,
		group: WindowedViewGroupId,
	) -> Result<WindowedViewGroup> {
		CatalogStore::find_windowed_view_group(rx, group)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"WindowedViewGroup with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				group
			)))
		})
	}
}
