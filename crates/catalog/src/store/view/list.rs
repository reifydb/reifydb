// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{Key, NamespaceId, ViewDef, ViewKey, ViewKind};
use reifydb_transaction::IntoStandardTransaction;

use crate::{CatalogStore, store::view::layout::view};

impl CatalogStore {
	pub async fn list_views_all(rx: &mut impl IntoStandardTransaction) -> crate::Result<Vec<ViewDef>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::new();

		let batch = txn.range_batch(ViewKey::full_scan(), 1024).await?;

		for entry in batch.items {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::View(view_key) = key {
					let view_id = view_key.view;

					let namespace_id =
						NamespaceId(view::LAYOUT.get_u64(&entry.values, view::NAMESPACE));

					let name = view::LAYOUT.get_utf8(&entry.values, view::NAME).to_string();

					let kind_value = view::LAYOUT.get_u8(&entry.values, view::KIND);
					let kind = if kind_value == 0 {
						ViewKind::Deferred
					} else {
						ViewKind::Transactional
					};

					let primary_key = Self::find_primary_key(&mut txn, view_id).await?;

					let columns = Self::list_columns(&mut txn, view_id).await?;

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
			}
		}

		Ok(result)
	}
}
