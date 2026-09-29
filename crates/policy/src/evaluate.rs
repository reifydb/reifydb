// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::expression::Expression;
use reifydb_value::{Result, value::identity::IdentityId};

pub trait PolicyEvaluator {
	fn evaluate_condition(
		&self,
		expr: &Expression,
		batch: &RecordBatch,
		row_count: usize,
		identity: IdentityId,
	) -> Result<bool>;
}
