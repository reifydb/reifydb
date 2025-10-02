// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	MultiVersionQueryTransaction, MultiVersionValues, NamespaceId, PrimaryKeyDef, PrimaryKeyId, ViewDef, ViewId,
	ViewKey, ViewKind,
};

use crate::{MaterializedCatalog, view::layout::view};

pub(crate) fn load_views(
	qt: &mut impl MultiVersionQueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = ViewKey::full_scan();
	let views = qt.range(range)?;

	for multi in views {
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
	let id = ViewId(view::LAYOUT.get_u64(&row, view::ID));
	let namespace = NamespaceId(view::LAYOUT.get_u64(&row, view::NAMESPACE));
	let name = view::LAYOUT.get_utf8(&row, view::NAME).to_string();

	let kind = match view::LAYOUT.get_u8(&row, view::KIND) {
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
	let pk_id_raw = view::LAYOUT.get_u64(&multi.values, view::PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
