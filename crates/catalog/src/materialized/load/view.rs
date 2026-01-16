// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, ViewId},
			key::PrimaryKeyDef,
			view::{ViewDef, ViewKind},
		},
		store::MultiVersionValues,
	},
	key::view::ViewKey,
};
use reifydb_transaction::standard::IntoStandardTransaction;

use crate::{
	materialized::MaterializedCatalog,
	store::view::layout::{
		view,
		view::{ID, KIND, NAME, NAMESPACE, PRIMARY_KEY},
	},
};

pub(crate) fn load_views(rx: &mut impl IntoStandardTransaction, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = ViewKey::full_scan();
	let mut stream = txn.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_view_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let view_def = convert_view(multi, primary_key);

		catalog.set_view(view_def.id, version, Some(view_def));
	}

	Ok(())
}

fn convert_view(multi: MultiVersionValues, primary_key: Option<PrimaryKeyDef>) -> ViewDef {
	let row = multi.values;
	let id = ViewId(view::LAYOUT.get_u64(&row, ID));
	let namespace = NamespaceId(view::LAYOUT.get_u64(&row, NAMESPACE));
	let name = view::LAYOUT.get_utf8(&row, NAME).to_string();

	let kind = match view::LAYOUT.get_u8(&row, KIND) {
		0 => ViewKind::Deferred,
		1 => ViewKind::Transactional,
		_ => unimplemented!(),
	};

	ViewDef {
		id,
		name,
		namespace,
		kind,
		columns: vec![],
		primary_key,
	}
}

fn get_view_primary_key_id(multi: &MultiVersionValues) -> Option<PrimaryKeyId> {
	let pk_id_raw = view::LAYOUT.get_u64(&multi.values, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
