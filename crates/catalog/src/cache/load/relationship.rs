// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::key::catalog::RelationshipKey;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::ValueError;

use super::CatalogCache;
use crate::store::relationship::list::decode_relationship_row;

pub fn load_relationships(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<(), ValueError> {
	let range = RelationshipKey::full_scan();

	let mut entries = Vec::new();
	{
		let stream = rx.range(range, RangeScope::All, 1024)?;
		for entry in stream {
			entries.push(entry?);
		}
	}

	for multi in entries {
		let rel = decode_relationship_row(EncodedCatalogRow::view(&multi.bytes))?;
		catalog.set_relationship(rel.id, multi.version, Some(rel));
	}

	Ok(())
}
