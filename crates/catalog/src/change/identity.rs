// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::identity::Identity,
	key::{EncodableKey, identity::IdentityKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

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
	let id = identity::SHAPE.get::<IdentityId>(bytes, identity::IDENTITY);
	let name = identity::SHAPE.get_utf8(bytes, identity::NAME).to_string();
	let enabled = identity::SHAPE.get::<bool>(bytes, identity::ENABLED);

	Identity {
		id,
		name,
		enabled,
	}
}
