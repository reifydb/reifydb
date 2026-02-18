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
	pub(crate) fn find_sumtype(
		rx: &mut impl AsTransaction,
		sumtype_id: SumTypeId,
	) -> crate::Result<Option<SumTypeDef>> {
		let mut txn = rx.as_transaction();
		let Some(multi) = txn.get(&SumTypeKey::encoded(sumtype_id))? else {
			return Ok(None);
		};

		Ok(Some(super::sumtype_def_from_row(&multi.values)))
	}

	pub(crate) fn find_sumtype_by_name(
		rx: &mut impl AsTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<SumTypeDef>> {
		let name = name.as_ref();
		let mut txn = rx.as_transaction();
		let mut stream = txn.range(NamespaceSumTypeKey::full_scan(namespace), 1024)?;

		let mut found_id = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let entry_name = sumtype_namespace::SCHEMA.get_utf8(row, sumtype_namespace::NAME);
			if name == entry_name {
				found_id =
					Some(SumTypeId(sumtype_namespace::SCHEMA.get_u64(row, sumtype_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(id) = found_id else {
			return Ok(None);
		};

		Ok(Some(Self::get_sumtype(&mut txn, id)?))
	}
}
