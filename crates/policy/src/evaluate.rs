// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::expression::Expression;
use reifydb_type::{Result, value::identity::IdentityId};

/// Trait for evaluating policy condition expressions.
///
/// Implemented by the engine to abstract over expression compilation and evaluation.
/// The policy crate calls this to check whether a condition passes for given rows.
pub trait PolicyEvaluator {
	/// Evaluate a condition expression against the given columns and identity.
	///
	/// Returns `true` if all rows pass the condition, `false` if any row is denied.
	fn evaluate_condition(
		&self,
		expr: &Expression,
		columns: &Columns,
		row_count: usize,
		identity: IdentityId,
	) -> Result<bool>;
}
