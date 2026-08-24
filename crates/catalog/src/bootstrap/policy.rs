// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	event::EventBus,
	interface::catalog::policy::{CallableOp, PolicyOpToCreate, PolicyTargetType, PolicyToCreate},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_value::value::identity::IdentityId;

use crate::{CatalogStore, Result};

const OPEN_CALL_BODY: &str = "filter { true }";

const OPEN_CALL_POLICIES: [(&str, &str, &str); 5] = [
	("system_call_rql_tokenize", "rql", "tokenize"),
	("system_call_rql_ast", "rql", "ast"),
	("system_call_rql_logical", "rql", "logical"),
	("system_call_rql_explain", "rql", "explain"),
	("system_call_graphql_explain", "graphql", "explain"),
];

pub fn bootstrap_call_policies(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	eventbus: &EventBus,
) -> Result<()> {
	let mut missing = Vec::new();
	{
		let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
		let mut rx = Transaction::Query(&mut qt);
		for (name, namespace, object) in OPEN_CALL_POLICIES {
			if CatalogStore::find_policy_by_name(&mut rx, name)?.is_none() {
				missing.push((name, namespace, object));
			}
		}
	}

	if missing.is_empty() {
		return Ok(());
	}

	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)?;

	for (name, namespace, object) in missing {
		CatalogStore::create_policy(
			&mut admin,
			PolicyToCreate {
				name: Some(name.to_string()),
				target_type: PolicyTargetType::Procedure,
				target_namespace: Some(namespace.to_string()),
				target_object: Some(object.to_string()),
				operations: vec![PolicyOpToCreate {
					operation: CallableOp::Call.as_str().to_string(),
					body_source: OPEN_CALL_BODY.to_string(),
				}],
			},
		)?;
	}
	admin.commit()?;

	Ok(())
}
