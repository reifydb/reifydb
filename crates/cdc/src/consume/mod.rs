// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Consumer side of the CDC stream. Each subscriber holds its own checkpoint, so a slow consumer never blocks
//! a fast one, and the checkpoint is persisted so a consumer that restarts resumes instead of re-reading. The
//! watermark it advances is what tells the producer side which records are safe to compact.

pub mod actor;
pub mod backlog;
pub mod checkpoint;
pub mod consumer;
pub mod host;
pub mod poll;
pub mod wake;
pub mod watermark;

use reifydb_core::{
	interface::cdc::{Cdc, SystemChange},
	key::{Key, kind::KeyKind},
};

pub fn is_relevant_cdc(cdc: &Cdc) -> bool {
	!cdc.changes.is_empty() || cdc.system_changes.iter().any(is_relevant_system_change)
}

fn is_relevant_system_change(change: &SystemChange) -> bool {
	let key = match change {
		SystemChange::Insert {
			key,
			..
		}
		| SystemChange::Update {
			key,
			..
		}
		| SystemChange::Delete {
			key,
			..
		} => key,
	};
	Key::kind(key)
		.map(|kind| {
			matches!(
				kind,
				KeyKind::Row
					| KeyKind::PartitionedRow | KeyKind::Flow
					| KeyKind::Operator | KeyKind::OperatorByFlow
					| KeyKind::FlowEdge | KeyKind::FlowEdgeByFlow
					| KeyKind::NamespaceFlow
			)
		})
		.unwrap_or(false)
}
