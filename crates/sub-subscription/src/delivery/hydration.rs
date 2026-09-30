// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, result::Result as StdResult};

use reifydb_core::{
	flow::{dag::FlowDag, operator::OperatorDef},
	interface::catalog::object::ObjectId,
};
use reifydb_engine::subscription::{HydrateError, HydrationBound};

pub(crate) fn backfill_sources(flow: &FlowDag) -> StdResult<BTreeSet<ObjectId>, HydrateError> {
	let mut sources = BTreeSet::new();
	for operator_id in flow.topological_order() {
		let Some(operator) = flow.get_operator(operator_id) else {
			continue;
		};
		match &operator.ty {
			OperatorDef::SourceTable {
				table,
				..
			} => {
				sources.insert(ObjectId::Table(*table));
			}
			OperatorDef::SourceView {
				view,
			} => {
				sources.insert(ObjectId::View(*view));
			}
			OperatorDef::SourceRingBuffer {
				ringbuffer,
				..
			} => {
				sources.insert(ObjectId::RingBuffer(*ringbuffer));
			}
			OperatorDef::SourceInlineData {
				..
			}
			| OperatorDef::SourceSeries {
				..
			} => return Err(HydrateError::UnsupportedSourceType),
			_ => {}
		}
	}
	Ok(sources)
}

pub(crate) fn hydration_bound(flow: &FlowDag) -> HydrationBound {
	let bounded = flow.topological_order().iter().any(|operator_id| {
		flow.get_operator(operator_id).is_some_and(|operator| matches!(operator.ty, OperatorDef::Take { .. }))
	});
	if bounded {
		HydrationBound::Pushed
	} else {
		HydrationBound::Absent
	}
}
