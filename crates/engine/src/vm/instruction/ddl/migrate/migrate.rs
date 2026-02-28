// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::migration::{MigrationAction, MigrationDef},
	internal_error,
	value::column::columns::Columns,
};
use reifydb_rql::{compiler::CompilationResult, nodes::MigrateNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{params::Params, value::Value};

use crate::{
	Result,
	vm::{services::Services, vm::Vm},
};

pub(crate) fn execute_migrate(
	vm: &mut Vm,
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	plan: MigrateNode,
	params: &Params,
) -> Result<Columns> {
	let txn = match tx {
		Transaction::Admin(txn) => txn,
		_ => {
			return Err(internal_error!("MIGRATE requires an admin transaction"));
		}
	};

	// List all migrations, sorted by name
	let mut migrations = services.catalog.list_migrations(&mut Transaction::Admin(&mut *txn))?;
	migrations.sort_by(|a, b| a.name.cmp(&b.name));

	// List all migration events to determine pending status
	let events = services.catalog.list_migration_events(&mut Transaction::Admin(&mut *txn))?;

	// Determine pending migrations: those whose latest event is not "Applied"
	let pending: Vec<MigrationDef> = migrations
		.into_iter()
		.filter(|m| {
			let latest = events.iter().filter(|e| e.migration_id == m.id).last();
			match latest {
				Some(e) => e.action != MigrationAction::Applied,
				None => true, // No events = never applied
			}
		})
		.collect();

	// Filter by target if specified
	let to_apply: Vec<MigrationDef> = if let Some(ref target) = plan.target {
		// Apply up to and including the target
		let mut result = Vec::new();
		for m in pending {
			result.push(m.clone());
			if m.name == *target {
				break;
			}
		}
		result
	} else {
		pending
	};

	let applied_count = to_apply.len();

	// Execute each migration body
	for migration in &to_apply {
		let compiled = services.compiler.compile(&mut Transaction::Admin(&mut *txn), &migration.body)?;

		match compiled {
			CompilationResult::Ready(compiled_list) => {
				let saved_ip = vm.ip;
				let mut migration_result = Vec::new();
				for compiled_unit in compiled_list.iter() {
					vm.ip = 0;
					vm.run(
						services,
						&mut Transaction::Admin(&mut *txn),
						&compiled_unit.instructions,
						params,
						&mut migration_result,
					)?;
				}
				vm.ip = saved_ip;
			}
			CompilationResult::Incremental(_) => {
				return Err(internal_error!("Migration '{}' body requires more input", migration.name));
			}
		}

		// Record "Applied" event
		services.catalog.create_migration_event(txn, migration, MigrationAction::Applied)?;
	}

	Ok(Columns::single_row([("migrations_applied", Value::Uint4(applied_count as u32))]))
}
