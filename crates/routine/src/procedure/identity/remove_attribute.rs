// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::value::column::columns::Columns;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{Value, value_type::ValueType},
};

use crate::{
	procedure::identity::set_attribute::{extract_args, extract_utf8_arg, resolve_attribute, resolve_identity},
	routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("identity::remove_attribute"));

pub struct RemoveIdentityAttribute;

impl Default for RemoveIdentityAttribute {
	fn default() -> Self {
		Self::new()
	}
}

impl RemoveIdentityAttribute {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for RemoveIdentityAttribute {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let args = extract_args("identity::remove_attribute", ctx.params, 2)?;
		let attribute_name = extract_utf8_arg("identity::remove_attribute", &args[1], 1)?;

		match ctx.tx {
			Transaction::Admin(admin) => {
				remove(ctx.catalog, admin, &args[0], &attribute_name, &ctx.fragment)
			}
			Transaction::Test(t) if ctx.identity.is_privileged() => {
				remove(ctx.catalog, t.inner, &args[0], &attribute_name, &ctx.fragment)
			}
			_ => Err(RoutineError::ProcedureExecutionFailed {
				procedure: Fragment::internal("identity::remove_attribute"),
				reason: "must run in an admin transaction".to_string(),
			}),
		}
	}
}

fn remove(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	user: &Value,
	attribute_name: &str,
	fragment: &Fragment,
) -> Result<Columns, RoutineError> {
	let identity = resolve_identity("identity::remove_attribute", catalog, txn, user, fragment)?;
	let attribute = resolve_attribute(catalog, txn, attribute_name, fragment)?;
	catalog.remove_identity_attribute_value(txn, identity.id, attribute.id)?;
	Ok(Columns::single_row([
		("identity", Value::Utf8(identity.name)),
		("attribute", Value::Utf8(attribute.name)),
		("removed", Value::Boolean(true)),
	]))
}
