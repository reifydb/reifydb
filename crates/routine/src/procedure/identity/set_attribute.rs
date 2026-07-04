// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_catalog::{
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_core::{
	interface::catalog::identity::{Identity, IdentityAttribute},
	value::column::columns::Columns,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, value_type::ValueType},
};

use crate::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("identity::set_attribute"));

pub struct SetIdentityAttribute;

impl Default for SetIdentityAttribute {
	fn default() -> Self {
		Self::new()
	}
}

impl SetIdentityAttribute {
	pub fn new() -> Self {
		Self
	}
}

pub(crate) fn extract_args(
	procedure: &'static str,
	params: &Params,
	expected: usize,
) -> Result<Vec<Value>, RoutineError> {
	match params {
		Params::Positional(args) if args.len() == expected => Ok(args.as_ref().clone()),
		Params::Positional(args) => Err(RoutineError::ProcedureArityMismatch {
			procedure: Fragment::internal(procedure),
			expected,
			actual: args.len(),
		}),
		_ => Err(RoutineError::ProcedureArityMismatch {
			procedure: Fragment::internal(procedure),
			expected,
			actual: 0,
		}),
	}
}

pub(crate) fn resolve_identity(
	procedure: &'static str,
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	user: &Value,
	fragment: &Fragment,
) -> Result<Identity, RoutineError> {
	let found = match user {
		Value::IdentityId(id) => catalog.find_identity(&mut Transaction::Admin(&mut *txn), *id)?,
		Value::Utf8(name) => {
			catalog.find_identity_by_name(&mut Transaction::Admin(&mut *txn), name.as_str())?
		}
		other => {
			return Err(RoutineError::ProcedureInvalidArgumentType {
				procedure: Fragment::internal(procedure),
				argument_index: 0,
				expected: vec![ValueType::IdentityId, ValueType::Utf8],
				actual: other.get_type(),
			});
		}
	};
	found.ok_or_else(|| {
		let name = match user {
			Value::Utf8(name) => name.as_str().to_string(),
			other => other.to_string(),
		};
		CatalogError::NotFound {
			kind: CatalogObjectKind::Identity,
			namespace: "system".to_string(),
			name,
			fragment: fragment.clone(),
		}
		.into()
	})
}

pub(crate) fn extract_utf8_arg(
	procedure: &'static str,
	value: &Value,
	argument_index: usize,
) -> Result<String, RoutineError> {
	match value {
		Value::Utf8(s) => Ok(s.as_str().to_string()),
		other => Err(RoutineError::ProcedureInvalidArgumentType {
			procedure: Fragment::internal(procedure),
			argument_index,
			expected: vec![ValueType::Utf8],
			actual: other.get_type(),
		}),
	}
}

pub(crate) fn resolve_attribute(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	name: &str,
	fragment: &Fragment,
) -> Result<IdentityAttribute, RoutineError> {
	catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut *txn), name)?.ok_or_else(|| {
		CatalogError::NotFound {
			kind: CatalogObjectKind::IdentityAttribute,
			namespace: "system".to_string(),
			name: name.to_string(),
			fragment: fragment.clone(),
		}
		.into()
	})
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for SetIdentityAttribute {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let args = extract_args("identity::set_attribute", ctx.params, 3)?;
		let attribute_name = extract_utf8_arg("identity::set_attribute", &args[1], 1)?;
		let value = extract_utf8_arg("identity::set_attribute", &args[2], 2)?;

		match ctx.tx {
			Transaction::Admin(admin) => {
				set(ctx.catalog, admin, &args[0], &attribute_name, &value, &ctx.fragment)
			}
			Transaction::Test(t) if ctx.identity.is_privileged() => {
				set(ctx.catalog, t.inner, &args[0], &attribute_name, &value, &ctx.fragment)
			}
			_ => Err(RoutineError::ProcedureExecutionFailed {
				procedure: Fragment::internal("identity::set_attribute"),
				reason: "must run in an admin transaction".to_string(),
			}),
		}
	}
}

fn set(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	user: &Value,
	attribute_name: &str,
	value: &str,
	fragment: &Fragment,
) -> Result<Columns, RoutineError> {
	let identity = resolve_identity("identity::set_attribute", catalog, txn, user, fragment)?;
	let attribute = resolve_attribute(catalog, txn, attribute_name, fragment)?;
	catalog.set_identity_attribute_value(txn, identity.id, attribute.id, value)?;
	Ok(Columns::single_row([
		("identity", Value::Utf8(identity.name)),
		("attribute", Value::Utf8(attribute.name)),
		("value", Value::Utf8(value.to_string())),
	]))
}
