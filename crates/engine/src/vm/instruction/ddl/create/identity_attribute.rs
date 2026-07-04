// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::error::CatalogError;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::{nodes::CreateIdentityAttributeNode, token::keyword::is_keyword};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::{Value, value_type::ValueType};

use crate::{Result, vm::services::Services};

pub(crate) fn create_identity_attribute(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateIdentityAttributeNode,
) -> Result<Columns> {
	let name = plan.name.text();

	if matches!(name, "id" | "name" | "roles") {
		return Err(CatalogError::IdentityAttributeNameInvalid {
			name: name.to_string(),
			reason: "reserved by $identity".to_string(),
			fragment: plan.name.clone(),
		}
		.into());
	} else if is_keyword(name) {
		return Err(CatalogError::IdentityAttributeNameInvalid {
			name: name.to_string(),
			reason: "collides with an RQL keyword".to_string(),
			fragment: plan.name.clone(),
		}
		.into());
	} else if name != name.to_lowercase() {
		return Err(CatalogError::IdentityAttributeNameInvalid {
			name: name.to_string(),
			reason: "must be lowercase".to_string(),
			fragment: plan.name.clone(),
		}
		.into());
	} else if plan.value_type != ValueType::Utf8 {
		return Err(CatalogError::IdentityAttributeTypeUnsupported {
			name: name.to_string(),
			value_type: plan.value_type,
			fragment: plan.name.clone(),
		}
		.into());
	}

	services.catalog.create_identity_attribute(txn, name, plan.value_type)?;

	Ok(Columns::single_row([("attribute", Value::Utf8(name.to_string())), ("created", Value::Boolean(true))]))
}
