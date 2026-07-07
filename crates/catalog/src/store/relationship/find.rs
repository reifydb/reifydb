// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::RelationshipId, relationship::Relationship},
	key::relationship::RelationshipKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::relationship::list::decode_relationship_row};

impl CatalogStore {
	pub(crate) fn find_relationship(rx: &mut Transaction<'_>, id: RelationshipId) -> Result<Option<Relationship>> {
		let multi = match rx.get(&RelationshipKey::encoded(id))? {
			Some(multi) => multi,
			None => return Ok(None),
		};
		Ok(Some(decode_relationship_row(&multi.row)?))
	}
}
