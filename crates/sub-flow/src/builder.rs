// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder pattern for configuring the flow subsystem

use std::path::PathBuf;

pub struct FlowBuilder {
	operators_dir: Option<PathBuf>,
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
		}
	}

	/// Set the directory to scan for FFI operator shared libraries
	pub fn operators_dir(mut self, path: PathBuf) -> Self {
		self.operators_dir = Some(path);
		self
	}

	/// Build the configuration (internal use only)
	pub(crate) fn build_config(self) -> FlowBuilderConfig {
		FlowBuilderConfig {
			operators_dir: self.operators_dir,
		}
	}
}

/// Internal configuration extracted from FlowBuilder
pub(crate) struct FlowBuilderConfig {
	pub operators_dir: Option<PathBuf>,
}
