// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropPolicyNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_policy(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropPolicyNode,
) -> crate::Result<Columns> {
	let name = plan.name.text();

	let policy = services.catalog.find_policy_by_name(&mut Transaction::Admin(&mut *txn), name)?;

	match policy {
		Some(policy) => {
			services.catalog.drop_policy(txn, policy.id)?;
			Ok(Columns::single_row([
				("policy", Value::Utf8(name.to_string())),
				("dropped", Value::Boolean(true)),
			]))
		}
		None => {
			if plan.if_exists {
				Ok(Columns::single_row([
					("policy", Value::Utf8(name.to_string())),
					("dropped", Value::Boolean(false)),
				]))
			} else {
				Err(reifydb_catalog::error::CatalogError::NotFound {
					kind: reifydb_catalog::error::CatalogObjectKind::Policy,
					namespace: "system".to_string(),
					name: name.to_string(),
					fragment: plan.name.clone(),
				}
				.into())
			}
		}
	}
}
