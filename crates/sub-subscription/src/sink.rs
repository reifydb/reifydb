// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_abi::flow::diff::DiffType;
use reifydb_core::{
	interface::{
		catalog::{flow::FlowNodeId, id::SubscriptionId, subscription::IMPLICIT_COLUMN_OP},
		change::{Change, Diff},
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_sub_flow::{
	operator::{Operator, Operators},
	transaction::FlowTransaction,
};
use reifydb_type::{Result, fragment::Fragment, value::row_number::RowNumber};

use crate::store::{PushResult, SubscriptionStore};

/// Ephemeral subscription sink operator that pushes Columns directly to
/// a SubscriptionStore buffer instead of writing encoded rows to persistent storage.
pub struct EphemeralSinkSubscriptionOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	subscription_id: SubscriptionId,
	store: Arc<SubscriptionStore>,
}

impl EphemeralSinkSubscriptionOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		subscription_id: SubscriptionId,
		store: Arc<SubscriptionStore>,
	) -> Self {
		Self {
			parent,
			node,
			subscription_id,
			store,
		}
	}

	/// Add implicit columns (_op) to the columns.
	fn add_implicit_columns(columns: &Columns, op: DiffType) -> Columns {
		let row_count = columns.row_count();

		let mut all_columns: Vec<Column> = columns.iter().cloned().collect();

		all_columns.push(Column {
			name: Fragment::internal(IMPLICIT_COLUMN_OP),
			data: ColumnData::uint1(vec![op as u8; row_count]),
		});

		Columns::with_row_numbers(all_columns, columns.row_numbers.to_vec())
	}
}

impl Operator for EphemeralSinkSubscriptionOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		for diff in change.diffs.iter() {
			let (columns, op) = match diff {
				Diff::Insert {
					post,
				} => (post, DiffType::Insert),
				Diff::Update {
					post,
					..
				} => (post, DiffType::Update),
				Diff::Remove {
					pre,
				} => (pre, DiffType::Remove),
			};

			let with_implicit = Self::add_implicit_columns(columns, op);
			match self.store.push(&self.subscription_id, with_implicit) {
				PushResult::Accepted => {}
				PushResult::NotFound => {
					// Subscription was unregistered before data arrived; this is benign.
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		unreachable!("EphemeralSinkSubscriptionOperator does not support pull")
	}
}
