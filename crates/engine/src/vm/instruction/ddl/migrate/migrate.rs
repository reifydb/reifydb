// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::migration::{Migration, MigrationAction},
	internal_error,
	value::column::columns::Columns,
};
use reifydb_rql::{
	compiler::{CompilationResult, Compiled, IncrementalCompilation},
	nodes::MigrateNode,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{
	Result,
	vm::{services::Services, vm::Vm},
};

pub(crate) fn execute_migrate(
	vm: &mut Vm,
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	plan: MigrateNode,
) -> Result<Columns> {
	let txn = require_admin_txn(tx)?;
	let pending = list_pending_migrations(services, txn)?;
	let to_apply = pick_migrations_to_apply(pending, plan.target.as_deref());

	let applied_count = to_apply.len();
	for migration in &to_apply {
		apply_migration(vm, services, txn, migration)?;
		services.catalog.create_migration_event(txn, migration, MigrationAction::Applied)?;
	}
	Ok(Columns::single_row([("migrations_applied", Value::Uint4(applied_count as u32))]))
}

#[inline]
fn require_admin_txn<'a>(tx: &'a mut Transaction<'_>) -> Result<&'a mut AdminTransaction> {
	match tx {
		Transaction::Admin(txn) => Ok(txn),
		Transaction::Test(t) => Ok(&mut *t.inner),
		_ => Err(internal_error!("MIGRATE requires an admin transaction")),
	}
}

/// List all migrations sorted by name and filter to those not yet `Applied`.
/// Migrations with no events are treated as pending.
#[inline]
fn list_pending_migrations(services: &Arc<Services>, txn: &mut AdminTransaction) -> Result<Vec<Migration>> {
	let mut migrations = services.catalog.list_migrations(&mut Transaction::Admin(&mut *txn))?;
	migrations.sort_by(|a, b| a.name.cmp(&b.name));
	let events = services.catalog.list_migration_events(&mut Transaction::Admin(&mut *txn))?;
	Ok(migrations
		.into_iter()
		.filter(|m| {
			events.iter()
				.find(|e| e.migration_id == m.id)
				.map(|e| e.action != MigrationAction::Applied)
				.unwrap_or(true)
		})
		.collect())
}

/// If `target` is set, return all pending migrations up to and including the
/// one whose name matches; otherwise return all pending.
#[inline]
fn pick_migrations_to_apply(pending: Vec<Migration>, target: Option<&str>) -> Vec<Migration> {
	let Some(target) = target else {
		return pending;
	};
	let mut result = Vec::new();
	for m in pending {
		let matches = m.name == target;
		result.push(m);
		if matches {
			break;
		}
	}
	result
}

#[inline]
fn apply_migration(
	vm: &mut Vm,
	services: &Arc<Services>,
	txn: &mut AdminTransaction,
	migration: &Migration,
) -> Result<()> {
	let compiled = services.compiler.compile(&mut Transaction::Admin(&mut *txn), &migration.body)?;
	match compiled {
		CompilationResult::Ready(compiled_list) => run_compiled_ready(vm, services, txn, &compiled_list),
		CompilationResult::Incremental(state) => run_compiled_incremental(vm, services, txn, state),
	}
}

#[inline]
fn run_compiled_ready(
	vm: &mut Vm,
	services: &Arc<Services>,
	txn: &mut AdminTransaction,
	compiled_list: &[Compiled],
) -> Result<()> {
	let saved_ip = vm.ip;
	let mut migration_result = Vec::new();
	for compiled_unit in compiled_list.iter() {
		vm.ip = 0;
		vm.run(
			services,
			&mut Transaction::Admin(&mut *txn),
			&compiled_unit.instructions,
			&mut migration_result,
		)?;
	}
	vm.ip = saved_ip;
	Ok(())
}

#[inline]
fn run_compiled_incremental(
	vm: &mut Vm,
	services: &Arc<Services>,
	txn: &mut AdminTransaction,
	mut state: IncrementalCompilation,
) -> Result<()> {
	let saved_ip = vm.ip;
	let mut migration_result = Vec::new();
	while let Some(compiled_unit) =
		services.compiler.compile_next(&mut Transaction::Admin(&mut *txn), &mut state)?
	{
		vm.ip = 0;
		vm.run(
			services,
			&mut Transaction::Admin(&mut *txn),
			&compiled_unit.instructions,
			&mut migration_result,
		)?;
	}
	vm.ip = saved_ip;
	Ok(())
}
