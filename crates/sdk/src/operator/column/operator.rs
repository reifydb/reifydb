// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::constraint::TypeConstraint;

#[derive(Debug, Clone)]
pub struct OperatorColumn {
	pub name: &'static str,
	pub type_constraint: TypeConstraint,
	pub description: &'static str,
}
