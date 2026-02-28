// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropUserNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_user(services: &Services, txn: &mut AdminTransaction, plan: DropUserNode) -> Result<Columns> {
	let name = plan.name.text();

	let user = services.catalog.find_user_by_name(&mut Transaction::Admin(&mut *txn), name)?;

	match user {
		Some(user) => {
			services.catalog.drop_user(txn, user.id)?;
			Ok(Columns::single_row([
				("user", Value::Utf8(name.to_string())),
				("dropped", Value::Boolean(true)),
			]))
		}
		None => {
			if plan.if_exists {
				Ok(Columns::single_row([
					("user", Value::Utf8(name.to_string())),
					("dropped", Value::Boolean(false)),
				]))
			} else {
				Err(CatalogError::NotFound {
					kind: CatalogObjectKind::User,
					namespace: "system".to_string(),
					name: name.to_string(),
					fragment: plan.name.clone(),
				}
				.into())
			}
		}
	}
}
