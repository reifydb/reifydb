// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, time::Duration};

use reifydb_core::{
	encoded::shape::RowShape,
	interface::catalog::config::{ConfigKey, GetConfig},
	key::{
		EncodableKey,
		system_version::{SystemVersion, SystemVersionKey},
	},
};
use reifydb_engine::{engine::StandardEngine, session::RetryStrategy};
use reifydb_runtime::actor::system::ActorSpawner;
use reifydb_store_multi::{
	MultiStore,
	gc::{
		historical::actor::spawn_historical_gc_actor, operator::actor::spawn_operator_settings_actor,
		row::actor::spawn_row_settings_actor,
	},
};
use reifydb_transaction::single::SingleTransaction;
use reifydb_value::{
	params::Params,
	value::{identity::IdentityId, value_type::ValueType},
};
use tracing::debug;

use crate::{MigrationStatement, Result};

const CURRENT_STORAGE_VERSION: u8 = 0x01;

/// Ensures the storage version key exists and matches the expected version.
/// On first boot, creates the version entry.
pub(crate) fn ensure_storage_version(single: &SingleTransaction) -> Result<()> {
	let shape = RowShape::testing(&[ValueType::Uint1]);
	let key = SystemVersionKey {
		version: SystemVersion::Storage,
	}
	.encode();

	let mut tx = single.begin_command([&key])?;

	match tx.get(&key)? {
		None => {
			let mut row = shape.allocate();
			shape.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
			tx.set(&key, row)?;
		}
		Some(single) => {
			let version = shape.get_u8(&single.row, 0);
			assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
		}
	};

	tx.commit()?;

	Ok(())
}

/// Spawns background actors during the bootload phase.
pub(crate) fn spawn_actors(engine: &StandardEngine, spawner: &ActorSpawner) -> Result<()> {
	// Spawn background actors
	let store = match engine.multi_owned().store() {
		MultiStore::Standard(s) => s.clone(),
	};

	let catalog = engine.catalog();

	store.configure_read_buffer_capacity(catalog.get_config_uint8(ConfigKey::MultiReadBufferCapacity) as usize);
	store.set_row_settings_provider(Arc::new(catalog.clone()));

	let _ttl_actor = spawn_row_settings_actor(store.clone(), spawner.clone(), catalog.clone());
	let _operator_ttl_actor = spawn_operator_settings_actor(store.clone(), spawner.clone(), catalog.clone());

	store.set_eviction_watermark(Arc::new(engine.clone()));

	let config: Arc<dyn GetConfig> = Arc::new(catalog);
	let _gc_actor = spawn_historical_gc_actor(store, spawner.clone(), engine.clone(), config);

	Ok(())
}

/// Registers migrations via idempotent `CREATE MIGRATION` and then runs `MIGRATE;`
/// to apply any pending ones.
///
/// Each `CREATE MIGRATION` is a no-op when a migration with the same name and
/// identical content hash is already registered, and returns `MigrationHashMismatch`
/// when the content has changed since registration.
pub(crate) fn apply_migrations(engine: &StandardEngine, migrations: &[MigrationStatement]) -> Result<()> {
	if migrations.is_empty() {
		return Ok(());
	}

	debug!("Applying {} registered migrations", migrations.len());

	for migration in migrations {
		match migration {
			MigrationStatement::Wrapped {
				name,
				body,
				rollback_body,
			} => {
				let mut rql = format!("CREATE MIGRATION '{}' {{", name);
				rql.push_str(body);
				rql.push('}');
				if let Some(rollback) = rollback_body.as_deref() {
					rql.push_str(" ROLLBACK {");
					rql.push_str(rollback);
					rql.push('}');
				}
				rql.push(';');
				run_admin_root(engine, &rql)?;
				debug!("Registered migration '{}'", name);
			}
			MigrationStatement::Raw(stmt) => {
				run_admin_root(engine, stmt)?;
				debug!("Registered raw migration statement ({} bytes)", stmt.len());
			}
		}
	}

	debug!("Running MIGRATE to apply pending migrations");
	let strategy =
		RetryStrategy::with_jittered_backoff(30, Duration::from_millis(10), Duration::from_millis(2_000));
	let rng = engine.rng();
	let result =
		strategy.execute(rng, "MIGRATE;", || engine.admin_as(IdentityId::root(), "MIGRATE;", Params::None));
	if let Some(e) = result.error {
		return Err(e);
	}
	if let Some(frame) = result.frames.first()
		&& let Ok(Some(count)) = frame.get::<u32>("migrations_applied", 0)
	{
		debug!("Applied {} pending migrations", count);
	}

	Ok(())
}

fn run_admin_root(engine: &StandardEngine, rql: &str) -> Result<()> {
	let result = engine.admin_as(IdentityId::root(), rql, Params::None);
	match result.error {
		Some(e) => Err(e),
		None => Ok(()),
	}
}
