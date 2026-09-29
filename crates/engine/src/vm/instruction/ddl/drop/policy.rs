// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::value::batch::single_row;
use reifydb_rql::nodes::DropPolicyNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_policy(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropPolicyNode,
) -> Result<RecordBatch> {
	let name = plan.name.text();

	let policy = services.catalog.find_policy_by_name(&mut Transaction::Admin(&mut *txn), name)?;

	match policy {
		Some(policy) => {
			services.catalog.drop_policy(txn, policy.id)?;
			single_row([("policy", Value::Utf8(name.to_string())), ("dropped", Value::Boolean(true))])
		}
		None => {
			if plan.if_exists {
				single_row([
					("policy", Value::Utf8(name.to_string())),
					("dropped", Value::Boolean(false)),
				])
			} else {
				Err(CatalogError::NotFound {
					kind: CatalogObjectKind::Policy,
					namespace: "system".to_string(),
					name: name.to_string(),
					fragment: plan.name.clone(),
				}
				.into())
			}
		}
	}
}
