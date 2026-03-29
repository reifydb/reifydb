// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::id::PrimaryKeyId,
	key::{EncodableKey, kind::KeyKind, view::ViewKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::view::{
		find::decode_view,
		shape::view::{PRIMARY_KEY, SHAPE},
	},
};

pub(super) struct ViewApplier;

impl CatalogChangeApplier for ViewApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let pk_raw = SHAPE.get_u64(row, PRIMARY_KEY);
		let primary_key = if pk_raw > 0 {
			catalog.materialized.find_primary_key_at(PrimaryKeyId(pk_raw), txn.version())
		} else {
			None
		};
		let view_id = ViewKey::decode(key).map(|k| k.view).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::View,
		})?;
		let columns = CatalogStore::list_columns(txn, view_id)?;
		let view = decode_view(row, columns, primary_key)?;
		catalog.materialized.set_view(view.id(), txn.version(), Some(view));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = ViewKey::decode(key).map(|k| k.view).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::View,
		})?;
		catalog.materialized.set_view(id, txn.version(), None);
		Ok(())
	}
}
