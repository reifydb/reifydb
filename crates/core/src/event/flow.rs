// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::path::PathBuf;

use reifydb_type::Type;

use crate::impl_event;

/// Information about a single column definition in an operator
#[derive(Debug, Clone)]
pub struct OperatorColumnDef {
	/// Column name
	pub name: String,
	/// Column type
	pub field_type: Type,
	/// Human-readable description
	pub description: String,
}

/// Emitted when a flow operator library is loaded
#[derive(Debug, Clone)]
pub struct FlowOperatorLoadedEvent {
	/// Name of the operator
	pub operator_name: String,
	/// Path to the shared library containing the operator
	pub library_path: PathBuf,
	/// API version of the operator
	pub api_version: u32,
	/// Semantic version of the operator
	pub operator_version: String,
	/// Human-readable description of the operator
	pub description: String,
	/// Input column definitions
	pub input: Vec<OperatorColumnDef>,
	/// Output column definitions
	pub output: Vec<OperatorColumnDef>,
}

impl_event!(FlowOperatorLoadedEvent);
