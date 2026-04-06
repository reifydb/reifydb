// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::shape::RowShape,
	key::{
		EncodableKey,
		system_version::{SystemVersion, SystemVersionKey},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::system::ActorSystem;
use reifydb_store_multi::{MultiStore, ttl::actor::spawn_row_ttl_actor};
use reifydb_transaction::single::SingleTransaction;
use reifydb_type::value::r#type::Type;


const CURRENT_STORAGE_VERSION: u8 = 0x01;

/// Ensures the storage version key exists and matches the expected version.
/// On first boot, creates the version entry.
pub(crate) fn ensure_storage_version(single: &SingleTransaction) -> crate::Result<()> {
	let shape = RowShape::testing(&[Type::Uint1]);
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
pub(crate) fn spawn_actors(engine: &StandardEngine, actor_system: &ActorSystem) -> crate::Result<()> {
	// Spawn background actors
	let store = match engine.multi_owned().store() {
		MultiStore::Standard(s) => s.clone(),
	};

	let catalog = engine.catalog();

	let _ttl_actor = spawn_row_ttl_actor(store, actor_system.clone(), catalog);

	Ok(())
}
