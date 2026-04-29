// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::constraint::TypeConstraint;

#[derive(Debug, Clone)]
pub struct OperatorColumn {
	/// Column name
	pub name: &'static str,
	/// Column type constraint
	pub type_constraint: TypeConstraint,
	/// Human-readable description
	pub description: &'static str,
}
