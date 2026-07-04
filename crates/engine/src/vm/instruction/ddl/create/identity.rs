// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashSet, sync::LazyLock};

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{
	interface::{catalog::identity::IdentityAttribute, evaluate::TargetColumn},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::{CreateIdentityNode, IdentityAttributeAssignment};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	params::Params,
	value::{Value, identity::IdentityId},
};

use crate::{
	Result,
	expression::{context::EvalContext, eval::evaluate},
	vm::{services::Services, stack::SymbolTable},
};

pub(crate) fn create_identity(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateIdentityNode,
	params: &Params,
) -> Result<Columns> {
	let name = plan.name.text();

	let resolved = resolve_attribute_assignments(services, txn, &plan.attributes, params)?;

	let identity = services.catalog.create_identity(
		txn,
		name,
		&services.runtime_context.clock,
		&services.runtime_context.rng,
	)?;

	for (attribute, value) in resolved {
		services.catalog.set_identity_attribute_value(txn, identity.id, &attribute, value)?;
	}

	Ok(Columns::single_row([("identity", Value::Utf8(name.to_string())), ("created", Value::Boolean(true))]))
}

pub(crate) fn resolve_attribute_assignments(
	services: &Services,
	txn: &mut AdminTransaction,
	assignments: &[IdentityAttributeAssignment],
	params: &Params,
) -> Result<Vec<(IdentityAttribute, Value)>> {
	let mut resolved = Vec::with_capacity(assignments.len());
	let mut seen = HashSet::new();
	for assignment in assignments {
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
		let value = evaluate_attribute_value(services, &attribute, assignment, params)?;
		resolved.push((attribute, value));
	}
	Ok(resolved)
}

fn evaluate_attribute_value(
	services: &Services,
	attribute: &IdentityAttribute,
	assignment: &IdentityAttributeAssignment,
	params: &Params,
) -> Result<Value> {
	static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

	let base = EvalContext {
		params,
		symbols: &EMPTY_SYMBOL_TABLE,
		routines: &services.routines,
		runtime_context: &services.runtime_context,
		arena: None,
		identity: IdentityId::root(),
		is_aggregate_context: false,
		columns: Columns::empty(),
		row_count: 1,
		target: None,
		take: None,
	};
	let mut eval_ctx = base.with_eval_empty();
	eval_ctx.target = Some(TargetColumn::Partial {
		source_name: None,
		column_name: None,
		column_type: attribute.value_type.clone(),
		properties: vec![],
	});
	let column = evaluate(&eval_ctx, &assignment.value)?;
	let value = column.data().get_value(0);
	if value.get_type() != attribute.value_type {
		return Err(CatalogError::IdentityAttributeValueInvalid {
			name: assignment.name.text().to_string(),
			expected: attribute.value_type.clone(),
			actual: value.get_type(),
			fragment: assignment.value.full_fragment_owned(),
		}
		.into());
	}
	Ok(value)
}
