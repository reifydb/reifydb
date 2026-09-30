// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::common::CommitVersion;
use reifydb_runtime::sync::mutex::Mutex;

use crate::store::{OperatorStore, StandardOperatorStore};

#[derive(Default)]
struct Pins {
	next: u64,
	versions: BTreeMap<u64, CommitVersion>,
}

#[derive(Clone)]
pub(crate) struct CheckpointPins(Arc<Mutex<Pins>>);

impl CheckpointPins {
	pub(crate) fn new() -> Self {
		Self(Arc::new(Mutex::new(Pins::default())))
	}

	pub(crate) fn floor(&self) -> Option<CommitVersion> {
		self.0.lock().versions.values().min().copied()
	}

	fn pin(&self, version: CommitVersion) -> CheckpointPin {
		let mut pins = self.0.lock();
		let id = pins.next;
		pins.next += 1;
		pins.versions.insert(id, version);
		CheckpointPin {
			pins: self.clone(),
			id,
		}
	}
}

pub struct CheckpointPin {
	pins: CheckpointPins,
	id: u64,
}

impl Drop for CheckpointPin {
	fn drop(&mut self) {
		self.pins.0.lock().versions.remove(&self.id);
	}
}

impl StandardOperatorStore {
	pub fn checkpoint_pin(&self, version: CommitVersion) -> CheckpointPin {
		self.pins.pin(version)
	}
}

impl OperatorStore {
	pub fn checkpoint_pin(&self, version: CommitVersion) -> CheckpointPin {
		match self {
			Self::Standard(store) => store.checkpoint_pin(version),
		}
	}
}
