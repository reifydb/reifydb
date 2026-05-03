// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::identity::GrantedRole,
	key::{EncodableKey, granted_role::GrantedRoleKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::granted_role::shape::granted_role};

pub(super) struct GrantedRoleApplier;

impl CatalogChangeApplier for GrantedRoleApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let gr = decode_granted_role(row);
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

fn decode_granted_role(row: &EncodedRow) -> GrantedRole {
	let identity = granted_role::SHAPE.get_identity_id(row, granted_role::IDENTITY);
	let role_id = granted_role::SHAPE.get_u64(row, granted_role::ROLE_ID);

	GrantedRole {
		identity,
		role_id,
	}
}
