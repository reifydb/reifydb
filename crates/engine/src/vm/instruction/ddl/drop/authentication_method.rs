// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::batch::single_row;
use reifydb_rql::nodes::DropAuthenticationMethodNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_authentication_method(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropAuthenticationMethodNode,
) -> Result<RecordBatch> {
	let name = plan.name.text();

	let Some(method) =
		services.catalog.find_authentication_method_by_name(&mut Transaction::Admin(&mut *txn), name)?
	else {
		if plan.if_exists {
			return single_row([
				("method", Value::Utf8(name.to_string())),
				("dropped", Value::Boolean(false)),
			]);
		}
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::AuthenticationMethod,
			namespace: "system".to_string(),
			name: name.to_string(),
			fragment: plan.name.clone(),
		}
		.into());
	};

	let credentials =
		services.catalog.list_authentications_by_method(&mut Transaction::Admin(&mut *txn), name)?.len();
	if credentials > 0 {
		return Err(CatalogError::AuthenticationMethodInUse {
			method: name.to_string(),
			credentials,
		}
		.into());
	}

	services.catalog.drop_authentication_method(txn, method.id)?;

	single_row([("method", Value::Utf8(name.to_string())), ("dropped", Value::Boolean(true))])
}
