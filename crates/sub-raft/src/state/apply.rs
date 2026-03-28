// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;

use reifydb_core::{
	event::{EventBus, transaction::PostCommitEvent},
	interface::store::{MultiVersionCommit, SingleVersionCommit},
	key::{Key, kind::KeyKind},
};
use reifydb_type::util::cowvec::CowVec;

use super::State;
use crate::{
	log::{Entry, Index},
	message::Command,
};

/// A Raft state machine that bridges committed log entries to ReifyDB's
/// storage. Dispatches multi-version writes to MultiStore and single-version
/// writes to SingleStore. Detects catalog key changes and invokes a callback.
pub struct Apply<M: MultiVersionCommit, S: SingleVersionCommit> {
	applied_index: Index,
	multi_store: M,
	single_store: S,
	event_bus: EventBus,
	on_catalog_change: Option<Box<dyn Fn() + Send>>,
	on_version_advance: Option<Box<dyn Fn(u64) + Send>>,
}

impl<M: MultiVersionCommit, S: SingleVersionCommit> Apply<M, S> {
	pub fn new(multi_store: M, single_store: S, event_bus: EventBus) -> Self {
		Self {
			applied_index: 0,
			multi_store,
			single_store,
			event_bus,
			on_catalog_change: None,
			on_version_advance: None,
		}
	}

	pub fn with_callbacks(
		multi_store: M,
		single_store: S,
		event_bus: EventBus,
		on_catalog_change: impl Fn() + Send + 'static,
		on_version_advance: impl Fn(u64) + Send + 'static,
	) -> Self {
		Self {
			applied_index: 0,
			multi_store,
			single_store,
			event_bus,
			on_catalog_change: Some(Box::new(on_catalog_change)),
			on_version_advance: Some(Box::new(on_version_advance)),
		}
	}
}

fn is_catalog_key(kind: KeyKind) -> bool {
	!matches!(kind, KeyKind::Row | KeyKind::IndexEntry)
}

impl<M: MultiVersionCommit + 'static, S: SingleVersionCommit + 'static> State for Apply<M, S> {
	fn get_applied_index(&self) -> Index {
		self.applied_index
	}

	fn apply(&mut self, entry: &Entry) {
		match &entry.command {
			Command::WriteMulti {
				deltas,
				version,
			} => {
				let cow_deltas = CowVec::new(deltas.clone());
				self.multi_store
					.commit(cow_deltas.clone(), *version)
					.expect("multi-store commit failed during raft apply");
				self.event_bus.emit(PostCommitEvent::new(cow_deltas, *version));

				if let Some(cb) = &self.on_version_advance {
					cb(version.0);
				}

				if self.on_catalog_change.is_some() {
					let has_catalog =
						deltas.iter().any(|d| Key::kind(d.key()).map_or(false, is_catalog_key));
					if has_catalog {
						(self.on_catalog_change.as_ref().unwrap())();
					}
				}
			}
			Command::WriteSingle {
				deltas,
			} => {
				let cow_deltas = CowVec::new(deltas.clone());
				self.single_store
					.commit(cow_deltas)
					.expect("single-store commit failed during raft apply");

				if let Some(cb) = &self.on_catalog_change {
					cb();
				}
			}
			Command::Noop => {}
		}
		self.applied_index = entry.index;
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}
