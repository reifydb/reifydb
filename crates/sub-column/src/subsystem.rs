// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
use reifydb_runtime::actor::mailbox::ActorRef;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_type::Result;
use tracing::{debug, info};

use crate::actor::{SeriesMessage, TableMessage};

#[derive(Clone, Debug)]
pub struct StorageConfig {
	pub table_tick_interval: Duration,
	pub series_tick_interval: Duration,
	// Default bucket width for series (in key units: ms/us/ns/s depending on
	// the series' `TimestampPrecision`, or plain u64 for integer keys).
	pub series_bucket_width: u64,
	// Wall-clock grace before a DateTime series bucket is considered closed.
	// Ignored for integer-keyed series.
	pub series_grace: Duration,
}

impl Default for StorageConfig {
	fn default() -> Self {
		Self {
			table_tick_interval: Duration::from_secs(1),
			series_tick_interval: Duration::from_secs(1),
			// 1 hour in nanoseconds - reasonable default for a DateTime series.
			series_bucket_width: 3_600 * 1_000_000_000,
			series_grace: Duration::from_secs(5),
		}
	}
}

// `StorageSubsystem` is a thin lifecycle marker: the factory spawns both actors
// during `create()`, and this struct just holds clones of their refs so
// `shutdown()` can deliver an explicit stop signal. Joining actually happens
// via actor-system shutdown on `Database::stop()` - same pattern as
// `MetricSubsystem`.
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
		Self {
			registry,
			table_ref,
			series_ref,
			running: Arc::new(AtomicBool::new(false)),
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

impl Subsystem for StorageSubsystem {
	fn name(&self) -> &'static str {
		"Storage"
	}

	fn start(&mut self) -> Result<()> {
		if self.running.swap(true, Ordering::SeqCst) {
			return Ok(());
		}
		info!("Storage (columnar materialization) subsystem started");
		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running.swap(false, Ordering::SeqCst) {
			return Ok(());
		}
		// Best-effort stop messages; actor-system shutdown is the authoritative
		// join point. A closed mailbox (`SendError::Closed`) means the actor has
		// already stopped, which is fine.
		let _ = self.table_ref.send(TableMessage::Shutdown);
		let _ = self.series_ref.send(SeriesMessage::Shutdown);
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
