// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_runtime::shutdown::Shutdown;
#[cfg(feature = "column")]
use reifydb_runtime::{actor::system::ActorHandle, shutdown::ShutdownErrors, sync::mutex::Mutex};
#[cfg(feature = "column")]
use reifydb_store_column::{compress::CompressionProfile, store::ColumnStore};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::{Result, value::duration::Duration};
use tracing::debug;
#[cfg(feature = "column")]
use tracing::info;

#[cfg(feature = "column")]
use crate::column::actor::{SeriesMessage, TableMessage};

#[derive(Clone, Debug)]
pub struct StorageConfig {
	pub table_tick_interval: Duration,
	pub series_tick_interval: Duration,

	pub series_bucket_width: u64,

	pub series_grace: Duration,

	pub series_merge_buckets: u64,

	pub series_merge_file_delay: Duration,

	pub materialize: bool,

	#[cfg(feature = "column")]
	pub compression: CompressionProfile,
}

impl Default for StorageConfig {
	fn default() -> Self {
		Self {
			table_tick_interval: Duration::from_seconds(1).unwrap(),
			series_tick_interval: Duration::from_seconds(1).unwrap(),

			series_bucket_width: 3_600 * 1_000_000_000,
			series_grace: Duration::from_seconds(5).unwrap(),
			series_merge_buckets: 1,
			series_merge_file_delay: Duration::from_seconds(60).unwrap(),

			materialize: false,

			#[cfg(feature = "column")]
			compression: CompressionProfile::Default,
		}
	}
}

#[cfg(feature = "column")]
pub struct MaterializationActors {
	pub table: ActorHandle<TableMessage>,
	pub series: ActorHandle<SeriesMessage>,
}

pub struct StorageSubsystem {
	#[cfg(feature = "column")]
	block_store: ColumnStore,
	#[cfg(feature = "column")]
	actors: Mutex<Option<MaterializationActors>>,
	running: Arc<AtomicBool>,
}

impl StorageSubsystem {
	#[cfg(feature = "column")]
	pub fn new(block_store: ColumnStore, actors: Option<MaterializationActors>) -> Self {
		info!("Storage (columnar materialization) subsystem started");
		Self {
			block_store,
			actors: Mutex::new(actors),
			running: Arc::new(AtomicBool::new(true)),
		}
	}

	#[cfg(not(feature = "column"))]
	pub fn new() -> Self {
		Self {
			running: Arc::new(AtomicBool::new(false)),
		}
	}

	#[cfg(feature = "column")]
	pub fn block_store(&self) -> ColumnStore {
		self.block_store.clone()
	}
}

#[cfg(not(feature = "column"))]
impl Default for StorageSubsystem {
	fn default() -> Self {
		Self::new()
	}
}

impl HasVersion for StorageSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-store".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "General storage subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Shutdown for StorageSubsystem {
	fn shutdown(&self) -> Result<()> {
		if !self.running.swap(false, Ordering::SeqCst) {
			return Ok(());
		}

		#[cfg(feature = "column")]
		if let Some(actors) = self.actors.lock().take() {
			let _ = actors.table.actor_ref().send_blocking(TableMessage::Shutdown);
			let _ = actors.series.actor_ref().send_blocking(SeriesMessage::Shutdown);
			let mut errors = ShutdownErrors::new("storage");
			if let Err(err) = actors.table.join() {
				errors.fail("table", err);
			}
			if let Err(err) = actors.series.join() {
				errors.fail("series", err);
			}
			errors.finish()?;
		}
		debug!("Storage subsystem stopped");
		Ok(())
	}
}

impl Subsystem for StorageSubsystem {
	fn name(&self) -> &'static str {
		"storage"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			HealthStatus::Healthy
		} else {
			HealthStatus::Failed {
				description: "Storage subsystem not running".to_string(),
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}
