// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::relationship::RelationshipKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::relationship::list::decode_relationship_row};

pub fn load_relationships(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = RelationshipKey::full_scan();

	let mut entries = Vec::new();
	{
		let stream = rx.range(range, 1024)?;
		for entry in stream {
			entries.push(entry?);
		}
	}

	for multi in entries {
		let rel = decode_relationship_row(&multi.row)?;
		catalog.set_relationship(rel.id, multi.version, Some(rel));
	}

	Ok(())
}
