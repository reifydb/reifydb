// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, sumtype::SumTypeDef},
	key::{namespace_sumtype::NamespaceSumTypeKey, sumtype::SumTypeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use crate::{CatalogStore, store::sumtype::schema::sumtype_namespace};

impl CatalogStore {
	pub(crate) fn find_sumtype(
		rx: &mut Transaction<'_>,
		sumtype_id: SumTypeId,
	) -> crate::Result<Option<SumTypeDef>> {
		let Some(multi) = rx.get(&SumTypeKey::encoded(sumtype_id))? else {
			return Ok(None);
		};

		Ok(Some(super::sumtype_def_from_row(&multi.values)))
	}

	pub(crate) fn find_sumtype_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<SumTypeDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceSumTypeKey::full_scan(namespace), 1024)?;

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

		Ok(Some(Self::get_sumtype(rx, id)?))
	}
}
