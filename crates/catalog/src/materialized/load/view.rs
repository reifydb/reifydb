// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	MultiVersionValues, NamespaceId, PrimaryKeyDef, PrimaryKeyId, QueryTransaction, ViewDef, ViewId, ViewKey,
	ViewKind,
};

use crate::{
	MaterializedCatalog,
	store::view::layout::{
		view,
		view::{ID, KIND, NAME, NAMESPACE, PRIMARY_KEY},
	},
};

pub(crate) async fn load_views(qt: &mut impl QueryTransaction, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = ViewKey::full_scan();
	let batch = qt.range(range).await?;

	for multi in batch.items {
		let version = multi.version;

		let pk_id = get_view_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key(id, version));
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
