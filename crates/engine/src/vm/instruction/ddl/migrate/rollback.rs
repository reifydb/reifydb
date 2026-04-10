// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::error::CatalogError;
use reifydb_core::{
	interface::catalog::migration::{Migration, MigrationAction},
	internal_error,
	value::column::columns::Columns,
};
use reifydb_rql::{compiler::CompilationResult, nodes::RollbackMigrationNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, params::Params, value::Value};

use crate::{
	Result,
	vm::{services::Services, vm::Vm},
};

pub(crate) fn execute_rollback_migration(
	vm: &mut Vm,
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	plan: RollbackMigrationNode,
	params: &Params,
) -> Result<Columns> {
	let txn = match tx {
		Transaction::Admin(txn) => txn,
		Transaction::Test(t) => &mut *t.inner,
		_ => {
			return Err(internal_error!("ROLLBACK MIGRATION requires an admin transaction"));
		}
	};

	// List all migrations, sorted by name descending (rollback in reverse order)
	let mut migrations = services.catalog.list_migrations(&mut Transaction::Admin(&mut *txn))?;
	migrations.sort_by(|a, b| b.name.cmp(&a.name));

	// List all migration events
	let events = services.catalog.list_migration_events(&mut Transaction::Admin(&mut *txn))?;

	// Determine applied migrations (latest event is "Applied"), in reverse name order
	let applied: Vec<Migration> = migrations
		.into_iter()
		.filter(|m| {
			let latest = events.iter().find(|e| e.migration_id == m.id);
			matches!(latest, Some(e) if e.action == MigrationAction::Applied)
		})
		.collect();

	// Determine which to rollback
	let to_rollback: Vec<Migration> = if let Some(ref target) = plan.target {
		// Rollback until we reach the target (exclusive — the target stays applied)
		let mut result = Vec::new();
		for m in applied {
			if m.name == *target {
				break;
			}
			result.push(m);
		}
		result
	} else {
		// Rollback the last applied migration only
		applied.into_iter().take(1).collect()
	};

	let rollback_count = to_rollback.len();

	// Execute each rollback body
	for migration in &to_rollback {
		let rollback_body = match &migration.rollback_body {
			Some(body) if !body.is_empty() => body.clone(),
			_ => {
				return Err(CatalogError::MigrationNoRollbackBody {
					name: migration.name.clone(),
					fragment: Fragment::None,
				}
				.into());
			}
		};

		let compiled = services.compiler.compile(&mut Transaction::Admin(&mut *txn), &rollback_body)?;

		match compiled {
			CompilationResult::Ready(compiled_list) => {
				let saved_ip = vm.ip;
				let mut rollback_result = Vec::new();
				for compiled_unit in compiled_list.iter() {
					vm.ip = 0;
					vm.run(
						services,
						&mut Transaction::Admin(&mut *txn),
						&compiled_unit.instructions,
						params,
						&mut rollback_result,
					)?;
				}
				vm.ip = saved_ip;
			}
			CompilationResult::Incremental(mut state) => {
				let saved_ip = vm.ip;
				let mut rollback_result = Vec::new();
				while let Some(compiled_unit) = services
					.compiler
					.compile_next(&mut Transaction::Admin(&mut *txn), &mut state)?
				{
					vm.ip = 0;
					vm.run(
						services,
						&mut Transaction::Admin(&mut *txn),
						&compiled_unit.instructions,
						params,
						&mut rollback_result,
					)?;
				}
				vm.ip = saved_ip;
			}
		}

		// Record "Rollback" event
		services.catalog.create_migration_event(txn, migration, MigrationAction::Rollback)?;
	}

	Ok(Columns::single_row([("migrations_rolled_back", Value::Uint4(rollback_count as u32))]))
}
