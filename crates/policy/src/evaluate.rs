// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{expression::Expression, value::column::columns::Columns};
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
