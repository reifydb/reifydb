// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::key::{EncodableKey, kind::KeyKind, relationship::RelationshipKey};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::relationship::list::decode_relationship_row};

pub(super) struct RelationshipApplier;

impl CatalogChangeApplier for RelationshipApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let rel = decode_relationship_row(bytes)?;
		catalog.cache.set_relationship(rel.id, txn.version(), Some(rel));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = RelationshipKey::decode(key).map(|k| k.relationship).ok_or(
			CatalogChangeError::KeyDecodeFailed {
				kind: KeyKind::Relationship,
			},
		)?;
		catalog.cache.set_relationship(id, txn.version(), None);
		Ok(())
	}
}
