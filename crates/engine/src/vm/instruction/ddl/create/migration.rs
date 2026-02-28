// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::migration::MigrationToCreate;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateMigrationNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_migration(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateMigrationNode,
) -> Result<Columns> {
	let migration = services.catalog.create_migration(
		txn,
		MigrationToCreate {
			name: plan.name.clone(),
			body: plan.body_source.clone(),
			rollback_body: plan.rollback_body_source.clone(),
		},
	)?;

	Ok(Columns::single_row([("migration", Value::Utf8(migration.name)), ("created", Value::Boolean(true))]))
}
