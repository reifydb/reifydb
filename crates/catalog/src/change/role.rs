// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::catalog::identity::Role,
	key::{EncodableKey, kind::KeyKind, role::RoleKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::role::shape::role};

pub(super) struct RoleApplier;

impl CatalogChangeApplier for RoleApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let r = decode_role(bytes);
		catalog.cache.set_role(r.id, txn.version(), Some(r));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = RoleKey::decode(key).map(|k| k.role).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Role,
		})?;
		catalog.cache.set_role(id, txn.version(), None);
		Ok(())
	}
}

fn decode_role(bytes: &EncodedBytes) -> Role {
	let id = role::get_id(bytes);
	let name = role::get_name(bytes).to_string();

	Role {
		id,
		name,
	}
}
