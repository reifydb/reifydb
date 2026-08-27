// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use reifydb_core::{
	interface::{catalog::id::SubscriptionId, change::StagedBatch},
	value::column::columns::Columns,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::value::diff_type::DiffType;

use crate::store::SubscriptionStore;

pub(crate) mod hydration;
mod pushdown;
pub(crate) mod sink;

pub struct DeliveryBuffer {
	store: Arc<SubscriptionStore>,
	staging: Mutex<HashMap<SubscriptionId, Vec<StagedBatch>>>,
}

impl DeliveryBuffer {
	pub fn new(store: Arc<SubscriptionStore>) -> Self {
		Self {
			store,
			staging: Mutex::new(HashMap::new()),
		}
	}

	pub fn push(&self, subscription_id: SubscriptionId, op: DiffType, columns: Columns) {
		self.staging.lock().entry(subscription_id).or_default().push((op, columns));
	}

	pub fn take_staged(&self, subscription_id: SubscriptionId) -> Vec<StagedBatch> {
		self.staging.lock().remove(&subscription_id).unwrap_or_default()
	}

	pub fn commit_batch(&self) {
		let staged = {
			let mut guard = self.staging.lock();
			mem::take(&mut *guard)
		};
		self.store.commit_staged(staged);
	}
}
