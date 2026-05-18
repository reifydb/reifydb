// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::NamespaceId,
			sumtype::{SumType, SumTypeKind, Variant},
		},
		store::MultiVersionRow,
	},
	key::sumtype::SumTypeKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;
use serde_json::from_str;
use tracing::warn;

use super::CatalogCache;
use crate::{
	Result,
	store::sumtype::shape::sumtype::{ID, KIND, NAME, NAMESPACE, SHAPE, VARIANTS_JSON},
};

pub(crate) fn load_sumtypes(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SumTypeKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let def = convert_sumtype(multi);
		catalog.set_sumtype(def.id, version, Some(def));
	}

	Ok(())
}

fn convert_sumtype(multi: MultiVersionRow) -> SumType {
	let row = multi.row;
	let id = SumTypeId(SHAPE.get_u64(&row, ID));
	let namespace = NamespaceId(SHAPE.get_u64(&row, NAMESPACE));
	let name = SHAPE.get_utf8(&row, NAME).to_string();
	let variants_json = SHAPE.get_utf8(&row, VARIANTS_JSON);
	let variants: Vec<Variant> = from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});

	let kind = if SHAPE.get_u8(&row, KIND) != 0 {
		SumTypeKind::Event
	} else {
		SumTypeKind::Enum
	};

	SumType {
		id,
		namespace,
		name,
		variants,
		kind,
	}
}
