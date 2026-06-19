// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{id::SubscriptionId, subscription::SubscriptionInspector},
	value::column::columns::Columns,
};
use reifydb_value::value::row_number::RowNumber;

use crate::store::SubscriptionStore;

pub(super) struct SubscriptionInspectorImpl {
	pub(super) store: Arc<SubscriptionStore>,
}

impl SubscriptionInspector for SubscriptionInspectorImpl {
	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.store.active_subscriptions()
	}

	fn column_count(&self, id: &SubscriptionId) -> Option<usize> {
		self.store.column_names(id).map(|v| v.len())
	}

	fn inspect(&self, id: SubscriptionId) -> Option<Columns> {
		let batches = self.store.drain(&id, usize::MAX);
		if batches.is_empty() {
			let names = self.store.column_names(&id)?;
			let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
			return Some(Columns::from_rows(&name_refs, &[]));
		}
		if batches.len() == 1 {
			return Some(batches.into_iter().next().unwrap());
		}

		let first = &batches[0];
		let names: Vec<&str> = first.iter().map(|c| c.name().text()).collect();

		let mut all_rows = Vec::new();
		let mut all_row_numbers = Vec::new();

		for batch in &batches {
			for i in 0..batch.row_count() {
				all_rows.push(batch.get_row(i));
				if i < batch.row_numbers.len() {
					all_row_numbers.push(batch.row_numbers[i]);
				} else {
					all_row_numbers.push(RowNumber(0));
				}
			}
		}

		Some(Columns::from_rows(&names, &all_rows).with_row_numbers(all_row_numbers))
	}
}
