// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, sumtype::SumType},
	key::{namespace_sumtype::NamespaceSumTypeKey, sumtype::SumTypeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use super::sumtype_from_row;
use crate::{CatalogStore, Result, store::sumtype::shape::sumtype_namespace};

impl CatalogStore {
	pub(crate) fn list_sumtypes(rx: &mut Transaction<'_>, namespace: NamespaceId) -> Result<Vec<SumType>> {
		let mut ids = Vec::new();
		{
			let mut stream = rx.range(NamespaceSumTypeKey::full_scan(namespace), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				let row = &multi.row;
				ids.push(SumTypeId(sumtype_namespace::SHAPE.get_u64(row, sumtype_namespace::ID)));
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

	pub(crate) fn list_all_sumtypes(rx: &mut Transaction<'_>) -> Result<Vec<SumType>> {
		let mut results = Vec::new();

		let mut stream = rx.range(SumTypeKey::full_scan(), 1024)?;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			results.push(sumtype_from_row(&multi.row));
		}

		Ok(results)
	}
}
