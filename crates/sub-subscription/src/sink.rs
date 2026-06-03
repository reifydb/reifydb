// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	mem,
	sync::Arc,
};

use postcard::{from_bytes, to_stdvec};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::{
	encoded::shape::RowShape,
	interface::{
		catalog::{flow::FlowNodeId, id::SubscriptionId, subscription::IMPLICIT_COLUMN_OP},
		change::{Change, Diff},
	},
	internal,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sub_flow::{
	operator::{
		Operator, OperatorCell,
		stateful::{raw::RawStatefulOperator, single::SingleStateful, utils},
	},
	transaction::{FlowTransaction, slot::PersistFn},
};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	reifydb_assertions,
	value::{blob::Blob, row_number::RowNumber, value_type::ValueType},
};
use serde::{Deserialize, Serialize};

use crate::store::SubscriptionStore;

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

	pub fn commit_batch(&self) {
		let staged = {
			let mut guard = self.staging.lock();
			mem::take(&mut *guard)
		};
		self.store.commit_staged(staged);
	}
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DeliveredState {
	rows: BTreeSet<RowNumber>,
}

pub struct EphemeralSinkSubscriptionOperator {
	#[allow(dead_code)]
	parent: OperatorCell,
	node: FlowNodeId,
	subscription_id: SubscriptionId,
	delivery: Arc<DeliveryBuffer>,
	shape: RowShape,
}

impl EphemeralSinkSubscriptionOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		subscription_id: SubscriptionId,
		delivery: Arc<DeliveryBuffer>,
	) -> Self {
		Self {
			parent,
			node,
			subscription_id,
			delivery,
			shape: RowShape::testing(&[ValueType::Blob]),
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

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let (mut state, persist) = self.take_delivered_state(txn)?;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_insert(&mut state, post),
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_update(&mut state, pre, post),
				Diff::Remove {
					pre,
					..
				} => self.apply_remove(&mut state, pre),
			}
		}

		txn.put_operator_state(self.node, state, persist);

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}
}

impl EphemeralSinkSubscriptionOperator {
	#[inline]
	fn take_delivered_state(&self, txn: &mut FlowTransaction) -> Result<(DeliveredState, PersistFn)> {
		let node_id = self.node;
		let shape_for_persist = self.shape.clone();

		txn.take_operator_state::<DeliveredState, _>(node_id, |txn| {
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
		})
	}

	fn apply_insert(&self, state: &mut DeliveredState, post: &Columns) {
		let row_count = post.row_count();
		let mut new_indices: Vec<usize> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			if state.rows.insert(post.row_numbers[row_idx]) {
				new_indices.push(row_idx);
			}
		}
		reifydb_assertions! {
			assert!(
				new_indices.len() <= row_count,
				"insert staged more rows than the diff carried, so a subscriber would receive phantom \
				 inserts not present in the source change (new_indices={}, row_count={row_count})",
				new_indices.len()
			);
		}
		if new_indices.len() == row_count {
			self.stage(post, DiffType::Insert);
		} else if !new_indices.is_empty() {
			let sub_post = post.extract_by_indices(&new_indices);
			self.stage(&sub_post, DiffType::Insert);
		}
	}

	fn apply_update(&self, state: &mut DeliveredState, pre: &Columns, post: &Columns) {
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
		reifydb_assertions! {
			assert!(
				update_indices.len() + insert_indices.len() == row_count,
				"update classification dropped or double-counted a post row, so a subscriber would miss a \
				 change or see it twice; every post row must be exactly one of update-or-insert \
				 (update={}, insert={}, row_count={row_count})",
				update_indices.len(),
				insert_indices.len()
			);
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

	fn apply_remove(&self, state: &mut DeliveredState, pre: &Columns) {
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
