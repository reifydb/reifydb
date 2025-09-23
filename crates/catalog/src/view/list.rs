// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Key, NamespaceId, QueryTransaction, ViewDef, ViewKey, ViewKind};

use crate::{CatalogStore, view::layout::view};

impl CatalogStore {
	pub fn list_views_all(rx: &mut impl QueryTransaction) -> crate::Result<Vec<ViewDef>> {
		let mut result = Vec::new();

		let entries: Vec<_> = rx.range(ViewKey::full_scan())?.into_iter().collect();

		for entry in entries {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::View(view_key) = key {
					let view_id = view_key.view;

					let namespace_id =
						NamespaceId(view::LAYOSVT.get_u64(&entry.row, view::NAMESPACE));

					let name = view::LAYOSVT.get_utf8(&entry.row, view::NAME).to_string();

					let kind_value = view::LAYOSVT.get_u8(&entry.row, view::KIND);
					let kind = if kind_value == 0 {
						ViewKind::Deferred
					} else {
						ViewKind::Transactional
					};

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
			}
		}

		Ok(result)
	}
}
