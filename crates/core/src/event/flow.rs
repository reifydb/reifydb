// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::path::PathBuf;

use reifydb_type::value::constraint::TypeConstraint;

use crate::define_event;

#[derive(Debug, Clone)]
pub struct OperatorColumn {
	pub name: String,

	pub field_type: TypeConstraint,

	pub description: String,
}

define_event! {

	pub struct FlowOperatorLoadedEvent {

		pub operator: String,

		pub library_path: PathBuf,

		pub api: u32,

		pub version: String,

		pub description: String,

		pub input: Vec<OperatorColumn>,

		pub output: Vec<OperatorColumn>,

		pub capabilities: u32,
	}
}
