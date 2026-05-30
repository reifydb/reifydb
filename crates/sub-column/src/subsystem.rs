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

use reifydb_column::registry::SnapshotRegistry;
use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_runtime::{actor::mailbox::ActorRef, shutdown::Shutdown};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use tracing::{debug, info};

use crate::actor::{SeriesMessage, TableMessage};

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
	registry: SnapshotRegistry,
	table_ref: ActorRef<TableMessage>,
	series_ref: ActorRef<SeriesMessage>,
	running: Arc<AtomicBool>,
}

impl StorageSubsystem {
	pub fn new(
		registry: SnapshotRegistry,
		table_ref: ActorRef<TableMessage>,
		series_ref: ActorRef<SeriesMessage>,
	) -> Self {
		info!("Storage (columnar materialization) subsystem started");
		Self {
			registry,
			table_ref,
			series_ref,
			running: Arc::new(AtomicBool::new(true)),
		}
	}

	pub fn registry(&self) -> SnapshotRegistry {
		self.registry.clone()
	}
}

impl HasVersion for StorageSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-column".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Columnar snapshot materialization subsystem".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Shutdown for StorageSubsystem {
	fn shutdown(&self) {
		if !self.running.swap(false, Ordering::SeqCst) {
			return;
		}

		let _ = self.table_ref.send(TableMessage::Shutdown);
		let _ = self.series_ref.send(SeriesMessage::Shutdown);
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
