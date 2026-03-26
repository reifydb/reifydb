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
	Result,
	materialized::MaterializedCatalog,
	store::view::{
		find::decode_view,
		schema::view::{PRIMARY_KEY, SCHEMA},
	},
};

pub(crate) fn load_views(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = ViewKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_view_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let view = convert_view(multi, primary_key)?;

		catalog.set_view(view.id(), version, Some(view));
	}

	Ok(())
}

fn convert_view(multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> Result<View> {
	decode_view(&multi.row, vec![], primary_key)
}

fn get_view_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = SCHEMA.get_u64(&multi.row, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
