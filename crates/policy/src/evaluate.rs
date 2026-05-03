// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::expression::Expression;
use reifydb_type::{Result, value::identity::IdentityId};

pub trait PolicyEvaluator {
	fn evaluate_condition(
		&self,
		expr: &Expression,
		columns: &Columns,
		row_count: usize,
		identity: IdentityId,
	) -> Result<bool>;
}
