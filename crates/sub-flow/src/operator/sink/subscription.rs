// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::rc::Rc;

use reifydb_abi::flow::diff::FlowDiffType;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	encoded::{schema::{Schema, SchemaField}},
	interface::{
		catalog::{flow::FlowNodeId, subscription::IMPLICIT_COLUMN_OP},
		resolved::ResolvedSubscription,
	},
	key::subscription_row::SubscriptionRowKey,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_core::encoded::key::EncodedKey;
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
use reifydb_sdk::flow::{FlowChange, FlowDiff};
use reifydb_type::{fragment::Fragment, value::row_number::RowNumber};

use super::encode_row_at_index;
use crate::{
	Operator,
	operator::{
		Operators,
		stateful::counter::{Counter, CounterDirection},
	},
	transaction::FlowTransaction,
};
use reifydb_core::util::encoding::keycode::serializer::KeySerializer;

pub struct SinkSubscriptionOperator {
	#[allow(dead_code)]
	parent: Rc<Operators>,
	node: FlowNodeId,
	subscription: ResolvedSubscription,
	counter: Counter,
}

impl SinkSubscriptionOperator {
	pub fn new(parent: Rc<Operators>, node: FlowNodeId, subscription: ResolvedSubscription) -> Self {
		let counter_key = {
			let mut serializer = KeySerializer::new();
			serializer.extend_bytes(subscription.def().id.as_bytes());
			EncodedKey::new(serializer.finish())
		};

		Self {
			parent,
			node,
			subscription,
			counter: Counter::with_key(node, counter_key, CounterDirection::Descending),
		}
	}

	/// Add implicit columns (_op) to the columns
	fn add_implicit_columns(columns: &Columns, op: FlowDiffType) -> Columns {
		let row_count = columns.row_count();

		// Clone existing columns
		let mut all_columns: Vec<Column> = columns.iter().cloned().collect();

		// Add implicit _op column
		all_columns.push(Column {
			name: Fragment::internal(IMPLICIT_COLUMN_OP),
			data: ColumnData::uint1(vec![op as u8; row_count]),
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

		for diff in change.diffs.iter() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Add implicit _op column
					let with_implicit = Self::add_implicit_columns(post, FlowDiffType::Insert);

					// Derive and persist schema from columns with implicit fields
					let schema = {
						let catalog = txn.catalog();
						create_schema_from_columns(&with_implicit, catalog)?
					};

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						// Get unique, incrementing row number for this notification
						let row_number = self.counter.next(txn)?;

						let (_, encoded) =
							encode_row_at_index(&with_implicit, row_idx, &schema, row_number);

						let key = SubscriptionRowKey::encoded(subscription_def.id, row_number);
						txn.set(&key, encoded)?;
					}
				}
				FlowDiff::Update {
					pre: _pre,
					post,
				} => {
					// Add implicit _op column
					let with_implicit = Self::add_implicit_columns(post, FlowDiffType::Update);

					// Derive and persist schema from columns with implicit fields
					let schema = {
						let catalog = txn.catalog();
						create_schema_from_columns(&with_implicit, catalog)?
					};

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						// Get unique, incrementing row number for this notification
						let row_number = self.counter.next(txn)?;

						let (_, encoded) =
							encode_row_at_index(&with_implicit, row_idx, &schema, row_number);

						let key = SubscriptionRowKey::encoded(subscription_def.id, row_number);
						txn.set(&key, encoded)?;
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					// Add implicit _op column
					let with_implicit = Self::add_implicit_columns(pre, FlowDiffType::Remove);

					// Derive and persist schema from columns with implicit fields
					let schema = {
						let catalog = txn.catalog();
						create_schema_from_columns(&with_implicit, catalog)?
					};

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						// Get unique, incrementing row number for this notification
						let row_number = self.counter.next(txn)?;

						let (_, encoded) =
							encode_row_at_index(&with_implicit, row_idx, &schema, row_number);

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

/// Create and persist a schema from actual column data
fn create_schema_from_columns(columns: &Columns, catalog: &Catalog) -> reifydb_type::Result<Schema> {
	let fields: Vec<SchemaField> = columns
		.iter()
		.map(|col| SchemaField::unconstrained(col.name.to_string(), col.data().get_type()))
		.collect();

	catalog.schema.get_or_create(fields)
}
