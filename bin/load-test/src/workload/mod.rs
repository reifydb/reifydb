// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod join;
mod mixed;
mod ping;
mod read;
mod scan;
mod write;

use std::sync::Arc;

pub use join::JoinWorkload;
pub use mixed::MixedWorkload;
pub use ping::PingWorkload;
use rand::rngs::StdRng;
pub use read::ReadWorkload;
pub use scan::ScanWorkload;
pub use write::WriteWorkload;

use crate::{
	client::Operation,
	config::{Config, WorkloadPreset},
};

/// A query to run during setup or teardown
pub struct SetupQuery {
	/// The RQL statement
	pub rql: String,
	/// True for commands (DDL/DML), false for queries
	pub is_command: bool,
}

impl SetupQuery {
	/// Create a new command setup query
	pub fn command(rql: impl Into<String>) -> Self {
		Self {
			rql: rql.into(),
			is_command: true,
		}
	}

	/// Create a new query setup query
	#[allow(dead_code)]
	pub fn query(rql: impl Into<String>) -> Self {
		Self {
			rql: rql.into(),
			is_command: false,
		}
	}
}

/// Trait for workload implementations
pub trait Workload: Send + Sync {
	/// Human-readable description of the workload
	fn description(&self) -> &str;

	/// Setup queries to run before the benchmark
	fn setup_queries(&self) -> Vec<SetupQuery>;

	/// Generate the next operation for a worker
	fn next_operation(&self, rng: &mut StdRng, worker_id: usize) -> Operation;

	/// Teardown queries to run after the benchmark
	fn teardown_queries(&self) -> Vec<String>;
}

/// Create a workload from the given preset and configuration
pub fn create_workload(preset: WorkloadPreset, config: &Config) -> Arc<dyn Workload> {
	match preset {
		WorkloadPreset::Ping => Arc::new(PingWorkload::new()),
		WorkloadPreset::Read => Arc::new(ReadWorkload::new(config.table_size)),
		WorkloadPreset::Write => Arc::new(WriteWorkload::new(config.table_size)),
		WorkloadPreset::Mixed => Arc::new(MixedWorkload::new(config.table_size, 80, 20)),
		WorkloadPreset::Scan => Arc::new(ScanWorkload::new(config.table_size)),
		WorkloadPreset::Join => Arc::new(JoinWorkload::new(config.table_size)),
	}
}
