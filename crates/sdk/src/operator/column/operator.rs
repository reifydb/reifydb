// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::constraint::TypeConstraint;

#[derive(Debug, Clone)]
pub struct OperatorColumn {
	pub name: &'static str,
	pub type_constraint: TypeConstraint,
	pub description: &'static str,
}
