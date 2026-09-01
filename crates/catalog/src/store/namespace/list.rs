// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{id::NamespaceId, namespace::Namespace},
	key::{Key, namespace::NamespaceKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::namespace::shape::namespace};

impl CatalogStore {
	pub(crate) fn list_namespaces_all(rx: &mut Transaction<'_>) -> Result<Vec<Namespace>> {
		let mut result = Vec::new();

		let namespace_range = NamespaceKey::full_scan();

		let stream = rx.range(namespace_range, RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;
			if let Some(namespace_key) = NamespaceKey::decode(&entry.key) {
				let namespace_id = namespace_key.namespace;

				let name = namespace::get_name(EncodedCatalogRow::view(&entry.bytes)).to_string();
				let parent_id =
					NamespaceId(namespace::get_parent_id(EncodedCatalogRow::view(&entry.bytes)));
				let grpc = namespace::try_get_grpc(EncodedCatalogRow::view(&entry.bytes))
					.map(|s| s.to_string())
					.filter(|s| !s.is_empty());
				let local_name = namespace::try_get_local_name(EncodedCatalogRow::view(&entry.bytes))
					.filter(|s| !s.is_empty())
					.unwrap_or_else(|| name.rsplit_once("::").map(|(_, s)| s).unwrap_or(&name))
					.to_string();
				let namespace = if let Some(address) = grpc {
					let token = namespace::try_get_token(EncodedCatalogRow::view(&entry.bytes))
						.map(|s| s.to_string())
						.filter(|s| !s.is_empty());
					Namespace::Remote {
						id: namespace_id,
						name,
						local_name,
						parent_id,
						address,
						token,
					}
				} else {
					Namespace::Local {
						id: namespace_id,
						name,
						local_name,
						parent_id,
					}
				};

				result.push(namespace);
			}
		}

		result.push(Namespace::system());
		result.push(Namespace::default_namespace());

		Ok(result)
	}
}
