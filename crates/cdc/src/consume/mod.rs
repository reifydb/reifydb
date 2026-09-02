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
	key::kind::KeyKind,
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
	KeyKind::of(key)
		.map(|kind| {
			matches!(
				kind,
				KeyKind::Row
					| KeyKind::SeriesRow | KeyKind::PartitionedRow
					| KeyKind::PartitionedSeriesRow
					| KeyKind::ClusteredRow
					| KeyKind::PartitionedClusteredRow | KeyKind::Flow
					| KeyKind::Operator | KeyKind::OperatorByFlow
					| KeyKind::FlowEdge | KeyKind::FlowEdgeByFlow
					| KeyKind::NamespaceFlow
			)
		})
		.unwrap_or(false)
}
