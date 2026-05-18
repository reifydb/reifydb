// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{id::PrimaryKeyId, key::PrimaryKey, view::View},
		store::MultiVersionRow,
	},
	key::view::ViewKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	cache::CatalogCache,
	store::view::{
		find::decode_view,
		shape::view::{PRIMARY_KEY, SHAPE},
	},
};

pub(crate) fn load_views(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = ViewKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	let mut views = Vec::new();
	for entry in stream.by_ref() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_view_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let view = convert_view(multi, primary_key)?;
		views.push((view, version));
	}
	drop(stream);

	for (mut view, version) in views {
		*view.columns_mut() = CatalogStore::list_columns(rx, view.id())?;
		catalog.set_view(view.id(), version, Some(view));
	}

	Ok(())
}

fn convert_view(multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> Result<View> {
	decode_view(&multi.row, vec![], primary_key)
}

fn get_view_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = SHAPE.get_u64(&multi.row, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
