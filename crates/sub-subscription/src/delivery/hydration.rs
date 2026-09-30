// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, result::Result as StdResult};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	flow::{dag::FlowDag, operator::OperatorDef},
	interface::catalog::object::ObjectId,
	internal_err,
};
use reifydb_engine::subscription::{HydrateError, HydrationBound};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

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

pub(crate) fn sorted_view_source(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	sources: &BTreeSet<ObjectId>,
) -> Result<bool> {
	let mut only = sources.iter();
	let (Some(ObjectId::View(view)), None) = (only.next(), only.next()) else {
		return Ok(false);
	};
	match catalog.find_view(txn, *view)? {
		Some(found) => Ok(!found.sort().is_empty()),
		None => internal_err!("source view {} of a hydrating subscription is missing at its snapshot", view.0),
	}
}

pub(crate) fn hydration_bound(flow: &FlowDag) -> HydrationBound {
	let bounded = flow.topological_order().iter().any(|operator_id| {
		flow.get_operator(operator_id).is_some_and(|operator| matches!(operator.ty, OperatorDef::Take { .. }))
	});
	if bounded {
		HydrationBound::Present
	} else {
		HydrationBound::Absent
	}
}
