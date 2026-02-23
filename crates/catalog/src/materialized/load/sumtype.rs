// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::NamespaceId,
			sumtype::{SumTypeDef, SumTypeKind, VariantDef},
		},
		store::MultiVersionValues,
	},
	key::sumtype::SumTypeKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;
use tracing::warn;

use super::MaterializedCatalog;
use crate::store::sumtype::schema::sumtype::{ID, KIND, NAME, NAMESPACE, SCHEMA, VARIANTS_JSON};

pub(crate) fn load_sumtypes(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = SumTypeKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let def = convert_sumtype(multi);
		catalog.set_sumtype(def.id, version, Some(def));
	}

	Ok(())
}

fn convert_sumtype(multi: MultiVersionValues) -> SumTypeDef {
	let row = multi.values;
	let id = SumTypeId(SCHEMA.get_u64(&row, ID));
	let namespace = NamespaceId(SCHEMA.get_u64(&row, NAMESPACE));
	let name = SCHEMA.get_utf8(&row, NAME).to_string();
	let variants_json = SCHEMA.get_utf8(&row, VARIANTS_JSON);
	let variants: Vec<VariantDef> = serde_json::from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});

	let kind = if SCHEMA.get_u8(&row, KIND) != 0 {
		SumTypeKind::Event
	} else {
		SumTypeKind::Enum
	};

	SumTypeDef {
		id,
		namespace,
		name,
		variants,
		kind,
	}
}
