// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_abi::flow::diff::DiffType;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	encoded::{
		key::EncodedKey,
		schema::{Schema, SchemaField},
	},
	interface::{
		catalog::{flow::FlowNodeId, subscription::IMPLICIT_COLUMN_OP},
		change::{Change, Diff},
		resolved::ResolvedSubscription,
	},
	key::subscription_row::SubscriptionRowKey,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
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

pub struct SinkSubscriptionOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	subscription: ResolvedSubscription,
	counter: Counter,
}

impl SinkSubscriptionOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, subscription: ResolvedSubscription) -> Self {
		let counter_key = {
			let mut serializer = KeySerializer::new();
			serializer.extend_u64(subscription.def().id.0);
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
	fn add_implicit_columns(columns: &Columns, op: DiffType) -> Columns {
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
		change: Change,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<Change> {
		let subscription_def = self.subscription.def().clone();

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					// Add implicit _op column (already decoded at source)
					let with_implicit = Self::add_implicit_columns(post, DiffType::Insert);

					// Derive and persist schema from columns with implicit fields
					let schema = {
						let catalog = txn.catalog();
						create_schema_from_columns(&with_implicit, catalog)?
					};

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						// Get unique, incrementing row number for this notification
						let row_number = self.counter.next(txn)?;

						let (_, encoded) = encode_row_at_index(
							&with_implicit,
							row_idx,
							&schema,
							row_number,
						);

						let key = SubscriptionRowKey::encoded(subscription_def.id, row_number);
						txn.set(&key, encoded)?;
					}
				}
				Diff::Update {
					pre: _pre,
					post,
				} => {
					// Add implicit _op column (already decoded at source)
					let with_implicit = Self::add_implicit_columns(post, DiffType::Update);

					// Derive and persist schema from columns with implicit fields
					let schema = {
						let catalog = txn.catalog();
						create_schema_from_columns(&with_implicit, catalog)?
					};

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						// Get unique, incrementing row number for this notification
						let row_number = self.counter.next(txn)?;

						let (_, encoded) = encode_row_at_index(
							&with_implicit,
							row_idx,
							&schema,
							row_number,
						);

						let key = SubscriptionRowKey::encoded(subscription_def.id, row_number);
						txn.set(&key, encoded)?;
					}
				}
				Diff::Remove {
					pre,
				} => {
					// Add implicit _op column (already decoded at source)
					let with_implicit = Self::add_implicit_columns(pre, DiffType::Remove);

					// Derive and persist schema from columns with implicit fields
					let schema = {
						let catalog = txn.catalog();
						create_schema_from_columns(&with_implicit, catalog)?
					};

					let row_count = with_implicit.row_count();
					for row_idx in 0..row_count {
						// Get unique, incrementing row number for this notification
						let row_number = self.counter.next(txn)?;

						let (_, encoded) = encode_row_at_index(
							&with_implicit,
							row_idx,
							&schema,
							row_number,
						);

						let key = SubscriptionRowKey::encoded(subscription_def.id, row_number);
						txn.set(&key, encoded)?;
					}
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, Vec::new()))
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
