// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeSource, expression::Expression, interface::catalog::namespace::Namespace, internal_error,
};
use reifydb_rql::query::QueryPlan;

use crate::{Result, error::EngineError};

pub(crate) fn reject_event_time_assignment(
	kind: &'static str,
	namespace: &Namespace,
	name: &str,
	time: &TimeSource,
	input: &QueryPlan,
) -> Result<()> {
	let Some(column) = time.ts() else {
		return Ok(());
	};
	let QueryPlan::Patch(patch) = input else {
		return Err(internal_error!("update of {} {} has no patch at the top of its input", kind, name));
	};
	for assignment in &patch.assignments {
		let Expression::Alias(alias) = assignment else {
			continue;
		};
		if alias.alias.name() == column {
			return Err(EngineError::EventTimeImmutable {
				kind,
				object: format!("{}::{}", namespace.name(), name),
				column: column.to_string(),
				fragment: alias.alias.0.clone(),
			}
			.into());
		}
	}
	Ok(())
}
