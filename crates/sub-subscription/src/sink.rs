// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	mem,
	sync::{Arc, Mutex},
};

use postcard::{from_bytes, to_stdvec};
use reifydb_abi::flow::diff::DiffType;
use reifydb_core::{
	encoded::shape::RowShape,
	interface::{
		catalog::{flow::FlowNodeId, id::SubscriptionId, subscription::IMPLICIT_COLUMN_OP},
		change::{Change, Diff},
	},
	internal,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_sub_flow::{
	operator::{
		Operator, Operators,
		stateful::{raw::RawStatefulOperator, single::SingleStateful, utils},
	},
	transaction::{FlowTransaction, slot::PersistFn},
};
use reifydb_type::{
	Result,
	error::Error,
	fragment::Fragment,
	value::{blob::Blob, row_number::RowNumber, r#type::Type},
};
use serde::{Deserialize, Serialize};

use crate::store::SubscriptionStore;

/// Staged delivery buffer for subscription sinks.
///
/// During a single CDC-batch pass, every sink writes into an in-memory staging
/// map instead of pushing to the `SubscriptionStore` directly. At the end of
/// the pass, `commit_batch` drains the staging map and applies all pushes to
/// the store atomically (from the poller's point of view). This prevents the
/// poller from observing a partial batch - where, for example, one batch
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DeliveredState {
	rows: BTreeSet<RowNumber>,
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
	shape: RowShape,
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
			shape: RowShape::testing(&[Type::Blob]),
		}
	}

	fn load_delivered_state(&self, txn: &mut FlowTransaction) -> Result<DeliveredState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(DeliveredState::default());
		}

		let blob = self.shape.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(DeliveredState::default());
		}

		from_bytes(blob.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize DeliveredState: {}", e))))
	}

	/// Add implicit columns (_op) to the columns.
	fn add_implicit_columns(columns: &Columns, op: DiffType) -> Columns {
		let row_count = columns.row_count();

		let mut all_columns: Vec<ColumnWithName> =
			columns.iter().map(|c| ColumnWithName::new(c.name().clone(), c.data().clone())).collect();

		all_columns.push(ColumnWithName::new(
			Fragment::internal(IMPLICIT_COLUMN_OP),
			ColumnBuffer::uint1(vec![op as u8; row_count]),
		));

		Columns::with_system_columns(
			all_columns,
			columns.row_numbers.to_vec(),
			columns.created_at.to_vec(),
			columns.updated_at.to_vec(),
		)
	}

	fn stage(&self, columns: &Columns, op: DiffType) {
		let with_implicit = Self::add_implicit_columns(columns, op);
		self.delivery.push(self.subscription_id, with_implicit);
	}
}

impl RawStatefulOperator for EphemeralSinkSubscriptionOperator {}

impl SingleStateful for EphemeralSinkSubscriptionOperator {
	fn layout(&self) -> RowShape {
		self.shape.clone()
	}
}

impl Operator for EphemeralSinkSubscriptionOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let node_id = self.node;
		let shape_for_persist = self.shape.clone();

		let (mut state, persist) = txn.take_operator_state::<DeliveredState, _>(node_id, |txn| {
			let s = self.load_delivered_state(txn)?;
			let shape = shape_for_persist.clone();
			let persist: PersistFn = Box::new(move |txn, value| {
				let state = value.downcast::<DeliveredState>().expect("DeliveredState slot type");
				let serialized = to_stdvec(&*state).map_err(|e| {
					Error(Box::new(internal!("Failed to serialize DeliveredState: {}", e)))
				})?;
				let blob = Blob::from(serialized);
				let key = utils::empty_key();
				let mut row = utils::load_or_create_row(node_id, txn, &key, &shape)?;
				shape.set_blob(&mut row, 0, &blob);
				utils::save_row(node_id, txn, &key, row)?;
				Ok(())
			});
			Ok((s, persist))
		})?;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					let row_count = post.row_count();
					for row_idx in 0..row_count {
						state.rows.insert(post.row_numbers[row_idx]);
					}
					self.stage(post, DiffType::Insert);
				}
				Diff::Update {
					pre,
					post,
				} => {
					let row_count = post.row_count();
					let mut update_indices: Vec<usize> = Vec::new();
					let mut insert_indices: Vec<usize> = Vec::new();
					for row_idx in 0..row_count {
						let pre_rn = pre.row_numbers[row_idx];
						let post_rn = post.row_numbers[row_idx];
						if state.rows.contains(&pre_rn) {
							if pre_rn != post_rn {
								state.rows.remove(&pre_rn);
								state.rows.insert(post_rn);
							}
							update_indices.push(row_idx);
						} else {
							state.rows.insert(post_rn);
							insert_indices.push(row_idx);
						}
					}
					if !update_indices.is_empty() {
						let sub_post = post.extract_by_indices(&update_indices);
						self.stage(&sub_post, DiffType::Update);
					}
					if !insert_indices.is_empty() {
						let sub_post = post.extract_by_indices(&insert_indices);
						self.stage(&sub_post, DiffType::Insert);
					}
				}
				Diff::Remove {
					pre,
				} => {
					let row_count = pre.row_count();
					let mut remove_indices: Vec<usize> = Vec::new();
					for row_idx in 0..row_count {
						let pre_rn = pre.row_numbers[row_idx];
						if state.rows.remove(&pre_rn) {
							remove_indices.push(row_idx);
						}
					}
					if !remove_indices.is_empty() {
						let sub_pre = pre.extract_by_indices(&remove_indices);
						self.stage(&sub_pre, DiffType::Remove);
					}
				}
			}
		}

		txn.put_operator_state(node_id, state, persist);

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		unreachable!("EphemeralSinkSubscriptionOperator does not support pull")
	}
}
