// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::error::diagnostic::flow::flow_unsupported_aggregate_expression;
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::{Expression, name::display_label};
use reifydb_value::{Result, error::Error};

use crate::flow::aggregate::is_representable;

pub(crate) fn validate_flow_aggregations(routines: &Routines, aggregations: &[Expression]) -> Result<()> {
	if aggregations.is_empty() {
		return Err(Error(Box::new(flow_unsupported_aggregate_expression("<none>"))));
	}
	for expr in aggregations {
		if !is_representable(routines, expr) {
			let output = display_label(expr).text().to_string();
			return Err(Error(Box::new(flow_unsupported_aggregate_expression(&output))));
		}
	}
	Ok(())
}
