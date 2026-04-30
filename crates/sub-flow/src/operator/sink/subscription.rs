// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use postcard::{from_bytes, to_stdvec};
use reifydb_abi::flow::diff::DiffType;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	encoded::{
		key::EncodedKey,
		shape::{RowShape, RowShapeField},
	},
	interface::{
		catalog::{
			flow::FlowNodeId,
			subscription::{IMPLICIT_COLUMN_OP, SubscriptionId},
		},
		change::{Change, Diff},
		resolved::ResolvedSubscription,
	},
	internal,
	key::subscription_row::SubscriptionRowKey,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_type::{
	Result,
	error::Error,
	fragment::Fragment,
	value::{blob::Blob, row_number::RowNumber, r#type::Type},
};
use serde::{Deserialize, Serialize};

use super::encode_row_at_index;
use crate::{
	Operator,
	operator::{
		Operators,
		stateful::{
			counter::{Counter, CounterDirection},
			raw::RawStatefulOperator,
			single::SingleStateful,
			utils,
		},
	},
	transaction::{FlowTransaction, slot::PersistFn},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DeliveredState {
	rows: BTreeSet<RowNumber>,
}

pub struct SinkSubscriptionOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	subscription: ResolvedSubscription,
	counter: Counter,
	shape: RowShape,
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

	fn add_implicit_columns(columns: &Columns, op: DiffType) -> Columns {
		let row_count = columns.row_count();

		let mut all_columns: Vec<ColumnWithName> = columns.iter().cloned().collect();

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

	fn write_subscription_rows(
		&self,
		txn: &mut FlowTransaction,
		columns: &Columns,
		op: DiffType,
		subscription_id: SubscriptionId,
	) -> Result<()> {
		let with_implicit = Self::add_implicit_columns(columns, op);

		let shape = {
			let catalog = txn.catalog();
			create_shape_from_columns(&with_implicit, catalog)?
		};

		let row_count = with_implicit.row_count();
		for row_idx in 0..row_count {
			let row_number = self.counter.next(txn)?;

			let (_, encoded) = encode_row_at_index(&with_implicit, row_idx, &shape, row_number)?;

			let key = SubscriptionRowKey::encoded(subscription_id, row_number);
			txn.set(&key, encoded)?;
		}

		Ok(())
	}
}

impl RawStatefulOperator for SinkSubscriptionOperator {}

impl SingleStateful for SinkSubscriptionOperator {
	fn layout(&self) -> RowShape {
		self.shape.clone()
	}
}

impl Operator for SinkSubscriptionOperator {
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
				let state =
					value.downcast::<DeliveredState>().expect("DeliveredState slot type");
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

		let subscription_id = self.subscription.def().id;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					let row_count = post.row_count();
					for row_idx in 0..row_count {
						state.rows.insert(post.row_numbers[row_idx]);
					}
					self.write_subscription_rows(txn, post, DiffType::Insert, subscription_id)?;
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
						self.write_subscription_rows(
							txn,
							&sub_post,
							DiffType::Update,
							subscription_id,
						)?;
					}
					if !insert_indices.is_empty() {
						let sub_post = post.extract_by_indices(&insert_indices);
						self.write_subscription_rows(
							txn,
							&sub_post,
							DiffType::Insert,
							subscription_id,
						)?;
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
						self.write_subscription_rows(
							txn,
							&sub_pre,
							DiffType::Remove,
							subscription_id,
						)?;
					}
				}
			}
		}

		txn.put_operator_state(node_id, state, persist);

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		unreachable!()
	}
}

fn create_shape_from_columns(columns: &Columns, catalog: &Catalog) -> Result<RowShape> {
	let fields: Vec<RowShapeField> = columns
		.iter()
		.map(|col| RowShapeField::unconstrained(col.name.to_string(), col.data().get_type()))
		.collect();

	catalog.shape.get_or_create(fields)
}
