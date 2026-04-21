// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	mem,
	sync::{Arc, Mutex},
};

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

use crate::store::SubscriptionStore;

/// Staged delivery buffer for subscription sinks.
///
/// During a single CDC-batch pass, every sink writes into an in-memory staging
/// map instead of pushing to the `SubscriptionStore` directly. At the end of
/// the pass, `commit_batch` drains the staging map and applies all pushes to
/// the store atomically (from the poller's point of view). This prevents the
/// poller from observing a partial batch — where, for example, one batch
/// member's diff has been pushed but another's is still in flight.
///
/// Only one CDC pass runs at a time per subsystem (the `PollConsumer` is
/// single-threaded), so contention on `staging` is nil; the `Mutex` exists to
/// satisfy `Send + Sync` requirements from the sink operator path.
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

	/// Stage one diff payload for `subscription_id`. Called by the sink operator
	/// during flow processing.
	pub fn push(&self, subscription_id: SubscriptionId, columns: Columns) {
		self.staging.lock().unwrap().entry(subscription_id).or_default().push(columns);
	}

	/// Commit all staged diffs to the store as a single atomic batch. Safe to
	/// call with an empty staging map (no-op).
	pub fn commit_batch(&self) {
		let staged = {
			let mut guard = self.staging.lock().unwrap();
			mem::take(&mut *guard)
		};
		self.store.commit_staged(staged);
	}
}

/// Ephemeral subscription sink operator. Stages output diffs in a
/// `DeliveryBuffer` buffer; the surrounding CDC consumer is responsible for
/// calling `commit_batch` once all flows have processed.
pub struct EphemeralSinkSubscriptionOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	subscription_id: SubscriptionId,
	delivery: Arc<DeliveryBuffer>,
}

impl EphemeralSinkSubscriptionOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		subscription_id: SubscriptionId,
		delivery: Arc<DeliveryBuffer>,
	) -> Self {
		Self {
			parent,
			node,
			subscription_id,
			delivery,
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

		Columns::with_system_columns(
			all_columns,
			columns.row_numbers.to_vec(),
			columns.created_at.to_vec(),
			columns.updated_at.to_vec(),
		)
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
			self.delivery.push(self.subscription_id, with_implicit);
		}

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		unreachable!("EphemeralSinkSubscriptionOperator does not support pull")
	}
}
