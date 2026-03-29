// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::identity::Role,
	key::{EncodableKey, kind::KeyKind, role::RoleKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::role::schema::role};

pub(super) struct RoleApplier;

impl CatalogChangeApplier for RoleApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let r = decode_role(row);
		catalog.materialized.set_role(r.id, txn.version(), Some(r));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = RoleKey::decode(key).map(|k| k.role).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Role,
		})?;
		catalog.materialized.set_role(id, txn.version(), None);
		Ok(())
	}
}

fn decode_role(row: &EncodedRow) -> Role {
	let id = role::SCHEMA.get_u64(row, role::ID);
	let name = role::SCHEMA.get_utf8(row, role::NAME).to_string();

	Role {
		id,
		name,
	}
}
