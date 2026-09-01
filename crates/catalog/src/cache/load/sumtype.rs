// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::{
			id::NamespaceId,
			sumtype::{SumType, SumTypeKind, Variant},
		},
		store::MultiVersionRow,
	},
	key::catalog::SumTypeKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::sumtype::SumTypeId;
use serde_json::from_str;
use tracing::warn;

use super::CatalogCache;
use crate::{Result, store::sumtype::shape::sumtype};

pub(crate) fn load_sumtypes(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SumTypeKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let def = convert_sumtype(multi)?;
		catalog.set_sumtype(def.id, version, Some(def));
	}

	Ok(())
}

fn convert_sumtype(multi: MultiVersionRow) -> Result<SumType> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = SumTypeId(sumtype::get_id(&bytes));
	let namespace = NamespaceId(sumtype::get_namespace(&bytes));
	let name = sumtype::get_name(&bytes).to_string();
	let variants_json = sumtype::get_variants_json(&bytes);
	let variants: Vec<Variant> = from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});

	let kind = if sumtype::get_kind(&bytes) != 0 {
		SumTypeKind::Event
	} else {
		SumTypeKind::Enum
	};

	Ok(SumType {
		id,
		namespace,
		name,
		variants,
		kind,
	})
}
