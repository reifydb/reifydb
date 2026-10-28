// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::value::batch::single_row;
use reifydb_rql::nodes::{AlterAuthenticationMethodNode, AlterMethodTtl};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{
	Result,
	vm::{
		instruction::ddl::create::authentication_method::{
			check_promote_guest, configure_method, registered_provider,
		},
		services::Services,
	},
};

pub(crate) fn alter_authentication_method(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterAuthenticationMethodNode,
) -> Result<RecordBatch> {
	let name = plan.name.text();
	let mut method =
		services.catalog.get_authentication_method_by_name(&mut Transaction::Admin(&mut *txn), name)?;
	let provider = registered_provider(services, name)?;

	let mut merged = method.properties.clone();
	merged.extend(plan.properties);
	method.properties = configure_method(provider, name, &merged)?;
	if let AlterMethodTtl::Set(ttl) = plan.ttl {
		method.ttl = ttl;
	}
	if let Some(promote_guest) = plan.promote_guest {
		method.promote_guest = promote_guest;
	}
	check_promote_guest(provider, name, method.promote_guest)?;

	services.catalog.alter_authentication_method(txn, method)?;

	single_row([("method", Value::Utf8(name.to_string())), ("altered", Value::Boolean(true))])
}
