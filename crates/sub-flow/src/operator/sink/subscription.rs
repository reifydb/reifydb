// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::rc::Rc;

use reifydb_core::{
	encoded::named::EncodedValuesNamedLayout,
	interface::{
		catalog::{flow::FlowNodeId, subscription::IMPLICIT_COLUMN_OP},
		resolved::ResolvedSubscription,
	},
	key::subscription_row::SubscriptionRowKey,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
use reifydb_sdk::flow::{FlowChange, FlowDiff};
use reifydb_type::{fragment::Fragment, value::row_number::RowNumber};

use super::{coerce_subscription_columns, encode_row_at_index};
use crate::{Operator, operator::Operators, transaction::FlowTransaction};

/// Operation type constants
const OP_INSERT: u8 = 0;
const OP_UPDATE: u8 = 1;
const OP_DELETE: u8 = 2;

pub struct SinkSubscriptionOperator {
	#[allow(dead_code)]
	parent: Rc<Operators>,
	node: FlowNodeId,
	subscription: ResolvedSubscription,
}

impl SinkSubscriptionOperator {
	pub fn new(parent: Rc<Operators>, node: FlowNodeId, subscription: ResolvedSubscription) -> Self {
		Self {
			parent,
			node,
			subscription,
		}
	}

	/// Add implicit columns (_op) to the columns
	fn add_implicit_columns(columns: &Columns, op: u8) -> Columns {
		let row_count = columns.row_count();

		// Clone existing columns
		let mut all_columns: Vec<Column> = columns.iter().cloned().collect();

		// Add implicit _op column
		all_columns.push(Column {
			name: Fragment::internal(IMPLICIT_COLUMN_OP),
			data: ColumnData::uint1(vec![op; row_count]),
		});

		// Preserve row numbers
		Columns::with_row_numbers(all_columns, columns.row_numbers.to_vec())
	}
}

impl Operator for SinkSubscriptionOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
		let subscription_def = self.subscription.def().clone();
		let layout: EncodedValuesNamedLayout = (&subscription_def).into();

		for diff in change.diffs.iter() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Coerce columns to match subscription schema types (user columns only)
					let coerced = coerce_subscription_columns(post, &subscription_def.columns)?;

					// Add implicit columns
					let with_implicit = Self::add_implicit_columns(&coerced, OP_INSERT);

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						let (row_number, encoded) =
							encode_row_at_index(&with_implicit, row_idx, &layout);

						let key = SubscriptionRowKey::encoded(subscription_def.id, row_number);
						txn.set(&key, encoded)?;
					}
				}
				FlowDiff::Update {
					pre: _pre,
					post,
				} => {
					// For updates, we store only the post-state with operation type UPDATE
					// The row is updated in place at the same row number
					let coerced = coerce_subscription_columns(post, &subscription_def.columns)?;

					// Add implicit columns
					let with_implicit = Self::add_implicit_columns(&coerced, OP_UPDATE);

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						let (row_number, encoded) =
							encode_row_at_index(&with_implicit, row_idx, &layout);

						let key = SubscriptionRowKey::encoded(subscription_def.id, row_number);
						txn.set(&key, encoded)?;
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					// For deletes, we store the pre-state with operation type DELETE
					// The row is updated in place to mark it as deleted
					let coerced = coerce_subscription_columns(pre, &subscription_def.columns)?;

					// Add implicit columns
					let with_implicit = Self::add_implicit_columns(&coerced, OP_DELETE);

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						let (row_number, encoded) =
							encode_row_at_index(&with_implicit, row_idx, &layout);

						let key = SubscriptionRowKey::encoded(subscription_def.id, row_number);
						txn.set(&key, encoded)?;
					}
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, Vec::new()))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		unreachable!()
	}
}
