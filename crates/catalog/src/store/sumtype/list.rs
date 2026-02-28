// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, sumtype::SumTypeDef},
	key::{namespace_sumtype::NamespaceSumTypeKey, sumtype::SumTypeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use super::sumtype_def_from_row;
use crate::{CatalogStore, Result, store::sumtype::schema::sumtype_namespace};

impl CatalogStore {
	pub(crate) fn list_sumtypes(rx: &mut Transaction<'_>, namespace: NamespaceId) -> Result<Vec<SumTypeDef>> {
		let mut ids = Vec::new();
		{
			let mut stream = rx.range(NamespaceSumTypeKey::full_scan(namespace), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				let row = &multi.values;
				ids.push(SumTypeId(sumtype_namespace::SCHEMA.get_u64(row, sumtype_namespace::ID)));
			}
		}

		let mut results = Vec::new();
		for id in ids {
			if let Some(def) = Self::find_sumtype(rx, id)? {
				results.push(def);
			}
		}

		Ok(results)
	}

	pub(crate) fn list_all_sumtypes(rx: &mut Transaction<'_>) -> Result<Vec<SumTypeDef>> {
		let mut results = Vec::new();

		let mut stream = rx.range(SumTypeKey::full_scan(), 1024)?;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			results.push(sumtype_def_from_row(&multi.values));
		}

		Ok(results)
	}
}
