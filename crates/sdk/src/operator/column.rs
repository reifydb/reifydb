// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Column definitions for operator input/output schemas

use reifydb_type::value::constraint::TypeConstraint;

/// A single column definition in an operator's input/output
#[derive(Debug, Clone)]
pub struct OperatorColumnDef {
	/// Column name
	pub name: &'static str,
	/// Column type constraint
	pub field_type: TypeConstraint,
	/// Human-readable description
	pub description: &'static str,
}
