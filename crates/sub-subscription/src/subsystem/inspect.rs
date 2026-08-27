// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{id::SubscriptionId, subscription::SubscriptionInspector},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{
	fragment::Fragment,
	value::{row_number::RowNumber, system_columns::SystemColumns},
};

use crate::store::SubscriptionStore;

const OP_COLUMN: &str = "#op";

pub(super) struct SubscriptionInspectorImpl {
	pub(super) store: Arc<SubscriptionStore>,
}

impl SubscriptionInspectorImpl {
	fn with_op(columns: Columns, ops: Vec<u8>) -> Columns {
		let mut all: Vec<ColumnWithName> =
			columns.iter().map(|c| ColumnWithName::new(c.name().clone(), c.data().clone())).collect();
		all.push(ColumnWithName::new(Fragment::internal(OP_COLUMN), ColumnBuffer::uint1(ops)));
		Columns::with_system(
			all,
			SystemColumns::new(
				columns.row_numbers().to_vec(),
				Vec::new(),
				columns.created_at().to_vec(),
				columns.updated_at().to_vec(),
				columns.time().to_vec(),
			),
		)
	}
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
			let mut names = self.store.column_names(&id)?;
			names.push(OP_COLUMN.to_string());
			let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
			return Some(Columns::from_rows(&name_refs, &[]));
		}
		if batches.len() == 1 {
			let (op, columns) = batches.into_iter().next().unwrap();
			let ops = vec![op.as_u8(); columns.row_count()];
			return Some(Self::with_op(columns, ops));
		}

		let names: Vec<&str> = batches[0].1.iter().map(|c| c.name().text()).collect();

		let mut all_rows = Vec::new();
		let mut all_row_numbers = Vec::new();
		let mut all_ops = Vec::new();

		for (op, batch) in &batches {
			for i in 0..batch.row_count() {
				all_rows.push(batch.get_row(i));
				all_ops.push(op.as_u8());
				if i < batch.row_numbers().len() {
					all_row_numbers.push(batch.row_numbers()[i]);
				} else {
					all_row_numbers.push(RowNumber(0));
				}
			}
		}

		let merged = Columns::from_rows(&names, &all_rows).with_row_numbers(all_row_numbers);
		Some(Self::with_op(merged, all_ops))
	}
}
