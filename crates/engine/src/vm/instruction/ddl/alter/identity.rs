// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::AlterIdentityNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn alter_identity(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterIdentityNode,
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

	let mut resolved = Vec::with_capacity(plan.attributes.len());
	let mut seen = HashSet::new();
	for assignment in &plan.attributes {
		let key = assignment.name.text();
		if !seen.insert(key.to_string()) {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::IdentityAttribute,
				namespace: "system".to_string(),
				name: key.to_string(),
				fragment: assignment.name.clone(),
			}
			.into());
		}
		let found =
			services.catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut *txn), key)?;
		let Some(attribute) = found else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::IdentityAttribute,
				namespace: "system".to_string(),
				name: key.to_string(),
				fragment: assignment.name.clone(),
			}
			.into());
		};
		resolved.push((attribute, assignment.value.clone()));
	}

	for (attribute, value) in resolved {
		services.catalog.set_identity_attribute_value(txn, identity.id, attribute.id, &value)?;
	}

	Ok(Columns::single_row([("identity", Value::Utf8(name.to_string())), ("altered", Value::Boolean(true))]))
}
