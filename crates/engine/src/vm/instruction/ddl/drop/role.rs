// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropRoleNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_role(services: &Services, txn: &mut AdminTransaction, plan: DropRoleNode) -> crate::Result<Columns> {
	let name = plan.name.text();

	let role = services.catalog.find_role_by_name(&mut Transaction::Admin(&mut *txn), name)?;

	match role {
		Some(role) => {
			services.catalog.drop_role(txn, role.id)?;
			Ok(Columns::single_row([
				("role", Value::Utf8(name.to_string())),
				("dropped", Value::Boolean(true)),
			]))
		}
		None => {
			if plan.if_exists {
				Ok(Columns::single_row([
					("role", Value::Utf8(name.to_string())),
					("dropped", Value::Boolean(false)),
				]))
			} else {
				Err(reifydb_catalog::error::CatalogError::NotFound {
					kind: reifydb_catalog::error::CatalogObjectKind::Role,
					namespace: "system".to_string(),
					name: name.to_string(),
					fragment: plan.name.clone(),
				}
				.into())
			}
		}
	}
}
