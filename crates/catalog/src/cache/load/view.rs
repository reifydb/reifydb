// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::{
			id::{PrimaryKeyId, ViewId},
			object::ObjectId,
		},
		store::MultiVersionRow,
	},
	key::catalog::ViewKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	cache::CatalogCache,
	store::view::{find::decode_view, shape::view},
};

pub(crate) fn load_views(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = ViewKey::full_scan();
	let mut stream = rx.range(range, RangeScope::All, 1024)?;

	let mut rows = Vec::new();
	for entry in stream.by_ref() {
		let multi = entry?;
		let pk_id = get_view_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, multi.version));
		rows.push((multi, pk_id, primary_key));
	}
	drop(stream);

	for (multi, pk_id, primary_key) in rows {
		let bytes = EncodedCatalogRow::view(&multi.bytes);
		let id = ViewId(view::get_id(bytes));
		let columns = CatalogStore::list_columns(rx, id)?;
		let view = decode_view(rx, bytes, columns, primary_key)?;
		if let Some(pk) = pk_id {
			catalog.set_primary_key_object(ObjectId::View(view.id()), pk);
		}
		catalog.set_view(view.id(), multi.version, Some(view));
	}

	Ok(())
}

fn get_view_primary_key_id<K>(multi: &MultiVersionRow<K>) -> Option<PrimaryKeyId> {
	let pk_id_raw = view::get_primary_key(EncodedCatalogRow::view(&multi.bytes));
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
