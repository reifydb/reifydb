// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropIdentityNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_identity(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropIdentityNode,
) -> Result<Columns> {
	let name = plan.name.text();

	let identity = services.catalog.find_identity_by_name(&mut Transaction::Admin(&mut *txn), name)?;

	match identity {
		Some(identity) => {
			services.catalog.drop_identity(txn, identity.id)?;
			Ok(Columns::single_row([
				("identity", Value::Utf8(name.to_string())),
				("dropped", Value::Boolean(true)),
			]))
		}
		None => {
			if plan.if_exists {
				Ok(Columns::single_row([
					("identity", Value::Utf8(name.to_string())),
					("dropped", Value::Boolean(false)),
				]))
			} else {
				Err(CatalogError::NotFound {
					kind: CatalogObjectKind::Identity,
					namespace: "system".to_string(),
					name: name.to_string(),
					fragment: plan.name.clone(),
				}
				.into())
			}
		}
	}
}
