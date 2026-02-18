// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::sumtype::SumTypeDef, return_internal_error};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::value::sumtype::SumTypeId;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn get_sumtype(rx: &mut impl AsTransaction, sumtype: SumTypeId) -> crate::Result<SumTypeDef> {
		match Self::find_sumtype(rx, sumtype)? {
			Some(def) => Ok(def),
			None => return_internal_error!("SumType with ID {:?} not found in catalog.", sumtype),
		}
	}
}
