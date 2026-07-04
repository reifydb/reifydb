// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropIdentityAttributeNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_identity_attribute(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropIdentityAttributeNode,
) -> Result<Columns> {
	let name = plan.name.text();

	let attribute = services.catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut *txn), name)?;

	match attribute {
		Some(attribute) => {
			services.catalog.drop_identity_attribute(txn, attribute.id)?;
			Ok(Columns::single_row([
				("attribute", Value::Utf8(name.to_string())),
				("dropped", Value::Boolean(true)),
			]))
		}
		None => {
			if plan.if_exists {
				Ok(Columns::single_row([
					("attribute", Value::Utf8(name.to_string())),
					("dropped", Value::Boolean(false)),
				]))
			} else {
				Err(CatalogError::NotFound {
					kind: CatalogObjectKind::IdentityAttribute,
					namespace: "system".to_string(),
					name: name.to_string(),
					fragment: plan.name.clone(),
				}
				.into())
			}
		}
	}
}
