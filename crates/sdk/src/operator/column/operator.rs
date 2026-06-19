// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::constraint::TypeConstraint;

#[derive(Debug, Clone)]
pub struct OperatorColumn {
	pub name: &'static str,
	pub type_constraint: TypeConstraint,
	pub description: &'static str,
}
