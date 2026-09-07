// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod actor;
pub mod backlog;
pub mod checkpoint;
pub mod consumer;
pub mod host;
pub mod poll;
pub mod wake;
pub mod watermark;

use reifydb_core::{
	interface::cdc::{Cdc, CdcChange},
	key::tag::KeyTag,
};

pub fn is_relevant_cdc(cdc: &Cdc) -> bool {
	cdc.changes.iter().any(is_relevant_cdc_change)
}

fn is_relevant_cdc_change(change: &CdcChange) -> bool {
	let key = match change {
		CdcChange::Insert {
			key,
			..
		}
		| CdcChange::Update {
			key,
			..
		}
		| CdcChange::Delete {
			key,
			..
		} => key,
	};
	KeyTag::of(key)
		.map(|kind| {
			matches!(
				kind,
				KeyTag::Row
					| KeyTag::SeriesRow | KeyTag::PartitionedRow
					| KeyTag::PartitionedSeriesRow | KeyTag::SortedViewRow
					| KeyTag::PartitionedSortedViewRow | KeyTag::Flow
					| KeyTag::Operator | KeyTag::OperatorByFlow
					| KeyTag::FlowEdge | KeyTag::FlowEdgeByFlow
					| KeyTag::NamespaceFlow
			)
		})
		.unwrap_or(false)
}
