// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::path::PathBuf;

use reifydb_type::value::constraint::TypeConstraint;

use crate::define_event;

/// Information about a single column definition in an operator
#[derive(Debug, Clone)]
pub struct OperatorColumnDef {
	/// Column name
	pub name: String,
	/// Column type constraint
	pub field_type: TypeConstraint,
	/// Human-readable description
	pub description: String,
}

define_event! {
	/// Emitted when a flow operator library is loaded
	pub struct FlowOperatorLoadedEvent {
		/// Name of the operator
		pub operator: String,
		/// Path to the shared library containing the operator
		pub library_path: PathBuf,
		/// API version of the operator
		pub api: u32,
		/// Semantic version of the operator
		pub version: String,
		/// Human-readable description of the operator
		pub description: String,
		/// Input column definitions
		pub input: Vec<OperatorColumnDef>,
		/// Output column definitions
		pub output: Vec<OperatorColumnDef>,
		/// Capabilities bitflags
		pub capabilities: u32,
	}
}
