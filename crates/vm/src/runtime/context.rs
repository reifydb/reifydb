// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! VM configuration and execution context.

use reifydb_catalog::Catalog;

/// VM configuration.
#[derive(Debug, Clone)]
pub struct VmConfig {
	/// Maximum operand stack depth.
	pub max_operand_stack: usize,

	/// Maximum pipeline stack depth.
	pub max_pipeline_stack: usize,

	/// Maximum call stack depth.
	pub max_call_depth: usize,

	/// Maximum scope depth.
	pub max_scope_depth: usize,

	/// Batch size for table scans.
	pub batch_size: u64,
}

impl Default for VmConfig {
	fn default() -> Self {
		Self {
			max_operand_stack: 1024,
			max_pipeline_stack: 64,
			max_call_depth: 256,
			max_scope_depth: 256,
			batch_size: 1000,
		}
	}
}

/// Execution context providing external resources.
pub struct VmContext {
	/// VM configuration.
	pub config: VmConfig,

	/// Optional catalog for real storage lookups.
	pub catalog: Option<Catalog>,
}

impl VmContext {
	/// Create a new VM context with default configuration.
	pub fn new() -> Self {
		Self {
			config: VmConfig::default(),
			catalog: None,
		}
	}

	/// Create a new VM context with custom configuration.
	pub fn with_config(config: VmConfig) -> Self {
		Self {
			config,
			catalog: None,
		}
	}

	/// Create a new VM context with a catalog.
	pub fn with_catalog(catalog: Catalog) -> Self {
		Self {
			config: VmConfig::default(),
			catalog: Some(catalog),
		}
	}

	/// Create a new VM context with both custom config and catalog.
	pub fn with_config_and_catalog(config: VmConfig, catalog: Catalog) -> Self {
		Self {
			config,
			catalog: Some(catalog),
		}
	}
}

impl Default for VmContext {
	fn default() -> Self {
		Self::new()
	}
}
