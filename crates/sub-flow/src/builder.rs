// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Builder pattern for configuring the flow subsystem

use std::path::PathBuf;

pub struct FlowBuilder {
	operators_dir: Option<PathBuf>,
	num_workers: Option<usize>,
}

impl Default for FlowBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl FlowBuilder {
	/// Create a new FlowBuilder with default settings
	pub fn new() -> Self {
		Self {
			operators_dir: None,
			num_workers: None,
		}
	}

	/// Set the directory to scan for FFI operator shared libraries
	pub fn operators_dir(mut self, path: PathBuf) -> Self {
		self.operators_dir = Some(path);
		self
	}

	/// Set the number of worker threads for flow processing.
	/// Defaults to 1 if not set.
	pub fn num_workers(mut self, count: usize) -> Self {
		self.num_workers = Some(count);
		self
	}

	/// Build the configuration (internal use only)
	pub(crate) fn build_config(self) -> FlowBuilderConfig {
		FlowBuilderConfig {
			operators_dir: self.operators_dir,
			num_workers: self.num_workers.unwrap_or(1),
		}
	}
}

/// Internal configuration extracted from FlowBuilder
pub(crate) struct FlowBuilderConfig {
	pub operators_dir: Option<PathBuf>,
	pub num_workers: usize,
}
