// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
#[cfg(feature = "column")]
use reifydb_runtime::actor::mailbox::ActorRef;
use reifydb_runtime::shutdown::Shutdown;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_value::value::duration::Duration;
use tracing::debug;
#[cfg(feature = "column")]
use tracing::info;

#[cfg(feature = "column")]
use crate::column::{
	actor::{SeriesMessage, TableMessage},
	block_store::ColumnBlockStore,
};

#[derive(Clone, Debug)]
pub struct StorageConfig {
	pub table_tick_interval: Duration,
	pub series_tick_interval: Duration,

	pub series_bucket_width: u64,

	pub series_grace: Duration,
}

impl Default for StorageConfig {
	fn default() -> Self {
		Self {
			table_tick_interval: Duration::from_seconds(1).unwrap(),
			series_tick_interval: Duration::from_seconds(1).unwrap(),

			series_bucket_width: 3_600 * 1_000_000_000,
			series_grace: Duration::from_seconds(5).unwrap(),
		}
	}
}

pub struct StorageSubsystem {
	#[cfg(feature = "column")]
	block_store: ColumnBlockStore,
	#[cfg(feature = "column")]
	table_ref: ActorRef<TableMessage>,
	#[cfg(feature = "column")]
	series_ref: ActorRef<SeriesMessage>,
	running: Arc<AtomicBool>,
}

impl StorageSubsystem {
	#[cfg(feature = "column")]
	pub fn new(
		block_store: ColumnBlockStore,
		table_ref: ActorRef<TableMessage>,
		series_ref: ActorRef<SeriesMessage>,
	) -> Self {
		info!("Storage (columnar materialization) subsystem started");
		Self {
			block_store,
			table_ref,
			series_ref,
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
	pub fn block_store(&self) -> ColumnBlockStore {
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
	fn shutdown(&self) {
		if !self.running.swap(false, Ordering::SeqCst) {
			return;
		}

		#[cfg(feature = "column")]
		{
			let _ = self.table_ref.send(TableMessage::Shutdown);
			let _ = self.series_ref.send(SeriesMessage::Shutdown);
		}
		debug!("Storage subsystem shutdown signalled");
	}
}

impl Subsystem for StorageSubsystem {
	fn name(&self) -> &'static str {
		"Storage"
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
