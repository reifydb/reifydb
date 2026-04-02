// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::view::View,
	key::{Key, view::ViewKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn list_views_all(rx: &mut Transaction<'_>) -> Result<Vec<View>> {
		let mut result = Vec::new();

		// Collect view IDs first to avoid holding stream borrow
		let mut view_ids = Vec::new();
		{
			let stream = rx.range(ViewKey::full_scan(), 1024)?;
			for entry in stream {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key)
					&& let Key::View(view_key) = key
				{
					view_ids.push(view_key.view);
				}
			}
		}

		// Now fetch each view using the find_view method which handles all storage kinds
		for view_id in view_ids {
			if let Some(view) = Self::find_view(rx, view_id)? {
				result.push(view);
			}
		}

		Ok(result)
	}
}
