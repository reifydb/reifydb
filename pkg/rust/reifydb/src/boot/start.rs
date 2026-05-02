// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::catalog::config::GetConfig,
	key::{
		EncodableKey,
		system_version::{SystemVersion, SystemVersionKey},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::system::ActorSystem;
use reifydb_store_multi::{
	MultiStore,
	gc::{
		historical::{QueryWatermark, actor::spawn_historical_gc_actor},
		operator::actor::spawn_operator_ttl_actor,
		row::actor::spawn_row_ttl_actor,
	},
};
use reifydb_transaction::single::SingleTransaction;
use reifydb_type::value::r#type::Type;

use crate::Result;

const CURRENT_STORAGE_VERSION: u8 = 0x01;

/// Ensures the storage version key exists and matches the expected version.
/// On first boot, creates the version entry.
pub(crate) fn ensure_storage_version(single: &SingleTransaction) -> Result<()> {
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
pub(crate) fn spawn_actors(engine: &StandardEngine, actor_system: &ActorSystem) -> Result<()> {
	// Spawn background actors
	let store = match engine.multi_owned().store() {
		MultiStore::Standard(s) => s.clone(),
	};

	let catalog = engine.catalog();

	let _ttl_actor = spawn_row_ttl_actor(store.clone(), actor_system.clone(), catalog.clone());
	let _operator_ttl_actor = spawn_operator_ttl_actor(store.clone(), actor_system.clone(), catalog.clone());

	let watermark = EngineQueryWatermark {
		engine: engine.clone(),
	};
	let config: Arc<dyn GetConfig> = Arc::new(catalog);
	let _gc_actor = spawn_historical_gc_actor(store, actor_system.clone(), watermark, config);

	Ok(())
}

struct EngineQueryWatermark {
	engine: StandardEngine,
}

impl QueryWatermark for EngineQueryWatermark {
	fn query_done_until(&self) -> CommitVersion {
		self.engine.query_done_until()
	}
}
