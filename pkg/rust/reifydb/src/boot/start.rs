// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_cdc::storage::CdcStore;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	key::{
		EncodableKey,
		system_version::{SystemVersion, SystemVersionKey},
	},
};
use reifydb_engine::{engine::StandardEngine, session::RetryStrategy};
use reifydb_store_multi::MultiStore;
use reifydb_transaction::single::SingleTransaction;
use reifydb_value::{
	params::Params,
	value::{duration::Duration, identity::IdentityId, value_type::ValueType},
};
use tracing::info;

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
			shape.set::<u8>(&mut row, 0, CURRENT_STORAGE_VERSION);
			tx.set(&key, row)?;
		}
		Some(single) => {
			let version = shape.get::<u8>(&single.row, 0);
			assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
		}
	};

	tx.commit()?;

	Ok(())
}

/// Applies store tuning that must be in effect before migrations run. Lifecycle actors are owned by the
/// lifecycle subsystem, not by this bootload phase.
pub(crate) fn configure_store(engine: &StandardEngine) -> Result<()> {
	let store = match engine.multi_owned().store() {
		MultiStore::Standard(s) => s.clone(),
	};

	let catalog = engine.catalog();

	store.configure_read_buffer(
		catalog.get_config_uint8(ConfigKey::MultiReadBufferPages) as usize,
		catalog.get_config_uint8(ConfigKey::MultiReadBufferPageSize),
	);
	store.configure_wal_autocheckpoint(catalog.get_config_uint8(ConfigKey::MultiWalAutocheckpoint) as u32);
	if let Some(cdc_store) = engine.ioc().try_resolve::<CdcStore>() {
		cdc_store
			.configure_wal_autocheckpoint(catalog.get_config_uint8(ConfigKey::CdcWalAutocheckpoint) as u32);
	}
	store.set_row_settings_provider(Arc::new(catalog.clone()));

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

	info!("Applying {} registered migrations", migrations.len());

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
				info!("Registered migration '{}'", name);
			}
			MigrationStatement::Raw(stmt) => {
				run_admin_root(engine, stmt)?;
				info!("Registered raw migration statement ({} bytes)", stmt.len());
			}
		}
	}

	info!("Running MIGRATE to apply pending migrations");
	let strategy = RetryStrategy::with_jittered_backoff(
		30,
		Duration::from_milliseconds(10).unwrap(),
		Duration::from_milliseconds(2_000).unwrap(),
	);
	let rng = engine.rng();
	let result =
		strategy.execute(rng, "MIGRATE;", || engine.admin_as(IdentityId::root(), "MIGRATE;", Params::None));
	if let Some(e) = result.error {
		return Err(e);
	}
	if let Some(frame) = result.frames.first()
		&& let Ok(Some(count)) = frame.get::<u32>("migrations_applied", 0)
	{
		info!("Applied {} pending migrations", count);
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
