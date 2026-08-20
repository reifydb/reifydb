// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_runtime::sync::mutex::Mutex;

use crate::store::SubscriptionStore;

pub(crate) mod hydration;
mod pushdown;
pub(crate) mod sink;

pub struct DeliveryBuffer {
	store: Arc<SubscriptionStore>,
	staging: Mutex<HashMap<SubscriptionId, Vec<Columns>>>,
}

impl DeliveryBuffer {
	pub fn new(store: Arc<SubscriptionStore>) -> Self {
		Self {
			store,
			staging: Mutex::new(HashMap::new()),
		}
	}

	pub fn push(&self, subscription_id: SubscriptionId, columns: Columns) {
		self.staging.lock().entry(subscription_id).or_default().push(columns);
	}

	pub fn take_staged(&self, subscription_id: SubscriptionId) -> Vec<Columns> {
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
