// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::path::PathBuf;

use crate::impl_event;

/// Emitted when a flow operator library is loaded
#[derive(Debug, Clone)]
pub struct FlowOperatorLoadedEvent {
	/// Name of the operator
	pub operator_name: String,
	/// Path to the shared library containing the operator
	pub library_path: PathBuf,
	/// API version of the operator
	pub api_version: u32,
}

impl_event!(FlowOperatorLoadedEvent);
