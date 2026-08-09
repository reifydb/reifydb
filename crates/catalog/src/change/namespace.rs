// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
};
use reifydb_core::{
	interface::catalog::id::NamespaceId,
	key::{EncodableKey, kind::KeyKind, namespace::NamespaceKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::namespace::shape::namespace};

pub(super) struct NamespaceApplier;

impl CatalogChangeApplier for NamespaceApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let ns = decode_namespace(EncodedCatalogRow::view(bytes));
		catalog.cache.set_namespace(ns.id(), txn.version(), Some(ns));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = NamespaceKey::decode(key).map(|k| k.namespace).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Namespace,
		})?;
		catalog.cache.set_namespace(id, txn.version(), None);
		Ok(())
	}
}

use reifydb_core::interface::catalog::namespace::Namespace;

fn decode_namespace(bytes: &EncodedCatalogRow) -> Namespace {
	let id = NamespaceId(namespace::get_id(bytes));
	let name = namespace::get_name(bytes).to_string();
	let parent_id = NamespaceId(namespace::get_parent_id(bytes));
	let grpc = namespace::try_get_grpc(bytes).map(|s| s.to_string()).filter(|s| !s.is_empty());
	let local_name = namespace::try_get_local_name(bytes)
		.filter(|s| !s.is_empty())
		.unwrap_or_else(|| name.rsplit_once("::").map(|(_, s)| s).unwrap_or(&name))
		.to_string();

	if let Some(address) = grpc {
		let token = namespace::try_get_token(bytes).map(|s| s.to_string()).filter(|s| !s.is_empty());
		Namespace::Remote {
			id,
			name,
			local_name,
			parent_id,
			address,
			token,
		}
	} else {
		Namespace::Local {
			id,
			name,
			local_name,
			parent_id,
		}
	}
}
