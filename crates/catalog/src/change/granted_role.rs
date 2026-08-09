// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
};
use reifydb_core::{
	interface::catalog::identity::GrantedRole,
	key::{EncodableKey, granted_role::GrantedRoleKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::granted_role::shape::granted_role};

pub(super) struct GrantedRoleApplier;

impl CatalogChangeApplier for GrantedRoleApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let gr = decode_granted_role(EncodedCatalogRow::view(bytes));
		catalog.cache.set_granted_role(gr.identity, gr.role_id, txn.version(), Some(gr));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let k = GrantedRoleKey::decode(key).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::GrantedRole,
		})?;
		catalog.cache.set_granted_role(k.identity, k.role, txn.version(), None);
		Ok(())
	}
}

fn decode_granted_role(bytes: &EncodedCatalogRow) -> GrantedRole {
	let identity = granted_role::get_identity(bytes);
	let role_id = granted_role::get_role_id(bytes);

	GrantedRole {
		identity,
		role_id,
	}
}
