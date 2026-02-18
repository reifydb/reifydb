// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, sumtype::SumTypeDef},
	key::{namespace_sumtype::NamespaceSumTypeKey, sumtype::SumTypeKey},
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::value::sumtype::SumTypeId;

use crate::{CatalogStore, store::sumtype::schema::sumtype_namespace};

impl CatalogStore {
	pub(crate) fn list_sumtypes(
		rx: &mut impl AsTransaction,
		namespace: NamespaceId,
	) -> crate::Result<Vec<SumTypeDef>> {
		let mut txn = rx.as_transaction();
		let mut ids = Vec::new();
		{
			let mut stream = txn.range(NamespaceSumTypeKey::full_scan(namespace), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				let row = &multi.values;
				ids.push(SumTypeId(sumtype_namespace::SCHEMA.get_u64(row, sumtype_namespace::ID)));
			}
		}

		let mut results = Vec::new();
		for id in ids {
			if let Some(def) = Self::find_sumtype(&mut txn, id)? {
				results.push(def);
			}
		}

		Ok(results)
	}

	pub(crate) fn list_all_sumtypes(rx: &mut impl AsTransaction) -> crate::Result<Vec<SumTypeDef>> {
		let mut txn = rx.as_transaction();
		let mut results = Vec::new();

		let mut stream = txn.range(SumTypeKey::full_scan(), 1024)?;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			results.push(super::sumtype_def_from_row(&multi.values));
		}

		Ok(results)
	}
}
