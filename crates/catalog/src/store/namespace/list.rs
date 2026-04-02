// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, namespace::Namespace},
	key::{Key, namespace::NamespaceKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::namespace::shape::namespace};

impl CatalogStore {
	pub(crate) fn list_namespaces_all(rx: &mut Transaction<'_>) -> Result<Vec<Namespace>> {
		let mut result = Vec::new();

		let namespace_range = NamespaceKey::full_scan();

		let stream = rx.range(namespace_range, 1024)?;

		for entry in stream {
			let entry = entry?;
			if let Some(key) = Key::decode(&entry.key)
				&& let Key::Namespace(namespace_key) = key
			{
				let namespace_id = namespace_key.namespace;

				let name = namespace::SHAPE.get_utf8(&entry.row, namespace::NAME).to_string();
				let parent_id = NamespaceId(namespace::SHAPE.get_u64(&entry.row, namespace::PARENT_ID));
				let grpc = namespace::SHAPE
					.try_get_utf8(&entry.row, namespace::GRPC)
					.map(|s| s.to_string())
					.filter(|s| !s.is_empty());
				let local_name = namespace::SHAPE
					.try_get_utf8(&entry.row, namespace::LOCAL_NAME)
					.filter(|s| !s.is_empty())
					.unwrap_or_else(|| name.rsplit_once("::").map(|(_, s)| s).unwrap_or(&name))
					.to_string();
				let namespace = if let Some(address) = grpc {
					let token = namespace::SHAPE
						.try_get_utf8(&entry.row, namespace::TOKEN)
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
