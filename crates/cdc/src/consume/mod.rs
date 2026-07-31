// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Consumer side of the CDC stream. A consumer registers with the host actor, polls for new records past its
//! checkpoint, and advances a watermark so the producer side knows what is safe to compact. Each subscriber holds
//! its own checkpoint independently; a slow consumer never blocks a fast one.
//!
//! The checkpoint and watermark are persisted alongside the CDC log so a consumer that disappears and comes back
//! resumes from where it left off rather than re-reading.

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
					| KeyKind::FlowNode | KeyKind::FlowNodeByFlow
					| KeyKind::FlowEdge | KeyKind::FlowEdgeByFlow
					| KeyKind::NamespaceFlow
			)
		})
		.unwrap_or(false)
}
