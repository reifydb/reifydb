// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::relationship::RelationshipToCreate;
use reifydb_core::{
	interface::catalog::relationship::RelationshipJunction as CoreRelationshipJunction,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateRelationshipNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_relationship(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateRelationshipNode,
) -> Result<Columns> {
	let rel = services.catalog.create_relationship(
		txn,
		RelationshipToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace,
			source_table: plan.source_table,
			source_column: plan.source_column,
			target_table: plan.target_table,
			target_column: plan.target_column,
			junction: plan.junction.map(|j| CoreRelationshipJunction {
				table: j.table,
				source_column: j.source_column,
				target_column: j.target_column,
			}),
			cardinality: plan.cardinality,
		},
	)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(rel.id.0)),
		("namespace_id", Value::Uint8(rel.namespace.0)),
		("name", Value::Utf8(rel.name)),
		("cardinality", Value::Utf8(rel.cardinality.as_str().to_string())),
		("created", Value::Boolean(true)),
	]))
}
