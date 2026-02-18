// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::NamespaceId,
		view::{ViewDef, ViewKind},
	},
	key::{Key, view::ViewKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, store::view::schema::view};

impl CatalogStore {
	pub(crate) fn list_views_all(rx: &mut Transaction<'_>) -> crate::Result<Vec<ViewDef>> {
		let mut result = Vec::new();

		// Collect view data first to avoid holding stream borrow
		let mut view_data = Vec::new();
		{
			let mut stream = rx.range(ViewKey::full_scan(), 1024)?;
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key) {
					if let Key::View(view_key) = key {
						let view_id = view_key.view;

						let namespace_id = NamespaceId(
							view::SCHEMA.get_u64(&entry.values, view::NAMESPACE),
						);

						let name = view::SCHEMA.get_utf8(&entry.values, view::NAME).to_string();

						let kind_value = view::SCHEMA.get_u8(&entry.values, view::KIND);
						let kind = if kind_value == 0 {
							ViewKind::Deferred
						} else {
							ViewKind::Transactional
						};

						view_data.push((view_id, namespace_id, name, kind));
					}
				}
			}
		}

		// Now fetch additional details for each view
		for (view_id, namespace_id, name, kind) in view_data {
			let primary_key = Self::find_primary_key(rx, view_id)?;
			let columns = Self::list_columns(rx, view_id)?;

			let view_def = ViewDef {
				id: view_id,
				namespace: namespace_id,
				name,
				kind,
				columns,
				primary_key,
			};

			result.push(view_def);
		}

		Ok(result)
	}
}
