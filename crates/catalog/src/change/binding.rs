// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{EncodableKey, binding::BindingKey, kind::KeyKind},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{Result, catalog::Catalog, error::CatalogChangeError, store::binding::find::decode_binding};

pub(super) struct BindingApplier;

impl CatalogChangeApplier for BindingApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let id = BindingKey::decode(key).map(|k| k.binding).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Binding,
		})?;
		let binding = decode_binding(row);
		catalog.cache.set_binding(id, txn.version(), Some(binding));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = BindingKey::decode(key).map(|k| k.binding).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Binding,
		})?;
		catalog.cache.set_binding(id, txn.version(), None);
		Ok(())
	}
}
