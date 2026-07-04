// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::AlterIdentityNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{params::Params, value::Value};

use crate::{
	Result,
	vm::{instruction::ddl::create::identity::resolve_attribute_assignments, services::Services},
};

pub(crate) fn alter_identity(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterIdentityNode,
	params: &Params,
) -> Result<Columns> {
	let name = plan.name.text();

	let found = services.catalog.find_identity_by_name(&mut Transaction::Admin(&mut *txn), name)?;
	let Some(identity) = found else {
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Identity,
			namespace: "system".to_string(),
			name: name.to_string(),
			fragment: plan.name.clone(),
		}
		.into());
	};

	let resolved = resolve_attribute_assignments(services, txn, &plan.attributes, params)?;

	for (attribute, value) in resolved {
		services.catalog.set_identity_attribute_value(txn, identity.id, &attribute, value)?;
	}

	Ok(Columns::single_row([("identity", Value::Utf8(name.to_string())), ("altered", Value::Boolean(true))]))
}
