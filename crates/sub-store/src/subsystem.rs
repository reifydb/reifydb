// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

#[cfg(feature = "column")]
use reifydb_column::registry::SnapshotRegistry;
use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
#[cfg(feature = "column")]
use reifydb_runtime::actor::mailbox::ActorRef;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::Result;
use tracing::{debug, info};

#[cfg(feature = "column")]
use crate::column::actor::{SeriesMessage, TableMessage};

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
			table_tick_interval: Duration::from_secs(1),
			series_tick_interval: Duration::from_secs(1),

			series_bucket_width: 3_600 * 1_000_000_000,
			series_grace: Duration::from_secs(5),
		}
	}
}

pub struct StorageSubsystem {
	#[cfg(feature = "column")]
	registry: SnapshotRegistry,
	#[cfg(feature = "column")]
	table_ref: ActorRef<TableMessage>,
	#[cfg(feature = "column")]
	series_ref: ActorRef<SeriesMessage>,
	running: Arc<AtomicBool>,
}

impl StorageSubsystem {
	#[cfg(feature = "column")]
	pub fn new(
		registry: SnapshotRegistry,
		table_ref: ActorRef<TableMessage>,
		series_ref: ActorRef<SeriesMessage>,
	) -> Self {
		Self {
			registry,
			table_ref,
			series_ref,
			running: Arc::new(AtomicBool::new(false)),
		}
	}

	#[cfg(not(feature = "column"))]
	pub fn new() -> Self {
		Self {
			running: Arc::new(AtomicBool::new(false)),
		}
	}

	#[cfg(feature = "column")]
	pub fn registry(&self) -> SnapshotRegistry {
		self.registry.clone()
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

impl Subsystem for StorageSubsystem {
	fn name(&self) -> &'static str {
		"Storage"
	}

	fn start(&mut self) -> Result<()> {
		if self.running.swap(true, Ordering::SeqCst) {
			return Ok(());
		}
		info!("Storage subsystem started");
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running.swap(false, Ordering::SeqCst) {
			return Ok(());
		}

		#[cfg(feature = "column")]
		{
			let _ = self.table_ref.send(TableMessage::Shutdown);
			let _ = self.series_ref.send(SeriesMessage::Shutdown);
		}
		debug!("Storage subsystem shutdown signalled");
		Ok(())
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

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}
