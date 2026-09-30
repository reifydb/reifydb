// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_core::interface::{catalog::id::SubscriptionId, change::StagedBatch};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::value::diff_type::DiffType;

use crate::store::SubscriptionStore;

pub(crate) mod hydration;
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

	pub fn push(&self, subscription_id: SubscriptionId, op: DiffType, batch: RecordBatch) {
		self.staging.lock().entry(subscription_id).or_default().push((op, batch));
	}

	pub fn take_staged(&self, subscription_id: SubscriptionId) -> Vec<StagedBatch> {
		self.staging.lock().remove(&subscription_id).unwrap_or_default()
	}

	pub fn commit_batch(&self) {
		let staged: HashMap<SubscriptionId, Vec<StagedBatch>> =
			self.staging.lock().extract_if(|id, _| !self.store.is_hydrating(id)).collect();
		self.store.commit_staged(staged);
	}

	pub fn commit_for(&self, subscription_id: SubscriptionId) {
		let staged: HashMap<SubscriptionId, Vec<StagedBatch>> =
			self.staging.lock().remove_entry(&subscription_id).into_iter().collect();
		self.store.commit_staged(staged);
	}
}
