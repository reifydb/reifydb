// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	interface::catalog::identity::Identity,
	key::{EncodableKey, identity::IdentityKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::identity::shape::identity};

pub(super) struct IdentityApplier;

impl CatalogChangeApplier for IdentityApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let id_entity = decode_identity(bytes);
		catalog.cache.set_identity(id_entity.id, txn.version(), Some(id_entity));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = IdentityKey::decode(key).map(|k| k.identity).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Identity,
		})?;
		catalog.cache.set_identity(id, txn.version(), None);
		Ok(())
	}
}

fn decode_identity(bytes: &EncodedBytes) -> Identity {
	let id = identity::get_identity(bytes);
	let name = identity::get_name(bytes).to_string();
	let enabled = identity::get_enabled(bytes);

	Identity {
		id,
		name,
		enabled,
	}
}
