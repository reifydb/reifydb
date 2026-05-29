// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::expression::Expression;
use reifydb_value::{Result, value::identity::IdentityId};

pub trait PolicyEvaluator {
	fn evaluate_condition(
		&self,
		expr: &Expression,
		columns: &Columns,
		row_count: usize,
		identity: IdentityId,
	) -> Result<bool>;
}
