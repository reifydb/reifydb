// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{id::PrimaryKeyId, view::ViewDef},
		store::MultiVersionValues,
	},
	key::view::ViewKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	materialized::MaterializedCatalog,
	store::view::{
		find::decode_view_def,
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
		let view_def = convert_view(multi, primary_key);

		catalog.set_view(view_def.id(), version, Some(view_def));
	}

	Ok(())
}

fn convert_view(
	multi: MultiVersionValues,
	primary_key: Option<reifydb_core::interface::catalog::key::PrimaryKeyDef>,
) -> ViewDef {
	decode_view_def(&multi.values, vec![], primary_key)
}

fn get_view_primary_key_id(multi: &MultiVersionValues) -> Option<PrimaryKeyId> {
	let pk_id_raw = SCHEMA.get_u64(&multi.values, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
