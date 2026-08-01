// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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

pub struct SetupQuery {
	pub rql: String,
	/// True routes the statement through the command path (DDL/DML), false through the query path.
	pub is_command: bool,
}

impl SetupQuery {
	pub fn command(rql: impl Into<String>) -> Self {
		Self {
			rql: rql.into(),
			is_command: true,
		}
	}

	#[allow(dead_code)]
	pub fn query(rql: impl Into<String>) -> Self {
		Self {
			rql: rql.into(),
			is_command: false,
		}
	}
}

pub trait Workload: Send + Sync {
	fn description(&self) -> &str;

	/// Run once before the benchmark, on a single connection.
	fn setup_queries(&self) -> Vec<SetupQuery>;

	fn next_operation(&self, rng: &mut StdRng, worker_id: usize) -> Operation;

	/// Run once after the benchmark; failures are ignored.
	fn teardown_queries(&self) -> Vec<String>;
}

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
