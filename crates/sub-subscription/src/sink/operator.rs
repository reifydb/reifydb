// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, id::SubscriptionId, subscription::IMPLICIT_COLUMN_OP},
		change::{Change, Diff, DiffType},
		flow::OperatorCapability,
	},
	metrics::heap::HeapSize,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_flow::{
	operator::{Operator, stateful::raw::RawStatefulOperator},
	transaction::ephemeral::EphemeralTransaction,
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	fragment::Fragment,
	reifydb_assertions,
	value::{row_number::RowNumber, system_columns::SystemColumns},
};

use crate::sink::DeliveryBuffer;

#[operator_state]
#[derive(Debug, Clone, Default, HeapSize)]
struct DeliveredState {
	rows: BTreeSet<RowNumber>,
}

pub struct EphemeralSinkPlan {
	operator: OperatorId,
	subscription_id: SubscriptionId,
	delivery: Arc<DeliveryBuffer>,
}

pub struct EphemeralSinkSubscriptionOperator {
	plan: Arc<EphemeralSinkPlan>,
	state: DeliveredState,
}

impl EphemeralSinkSubscriptionOperator {
	pub fn new(
		operator: OperatorId,
		subscription_id: SubscriptionId,
		delivery: Arc<DeliveryBuffer>,
	) -> Self {
		Self {
			plan: Arc::new(EphemeralSinkPlan {
				operator,
				subscription_id,
				delivery,
			}),
			state: DeliveredState::default(),
		}
	}
}

impl EphemeralSinkPlan {
	fn add_implicit_columns(columns: &Columns, op: DiffType) -> Columns {
		let row_count = columns.row_count();

		let mut all_columns: Vec<ColumnWithName> =
			columns.iter().map(|c| ColumnWithName::new(c.name().clone(), c.data().clone())).collect();

		all_columns.push(ColumnWithName::new(
			Fragment::internal(IMPLICIT_COLUMN_OP),
			ColumnBuffer::uint1(vec![op as u8; row_count]),
		));

		Columns::with_system(
			all_columns,
			SystemColumns::new(
				columns.row_numbers().to_vec(),
				Vec::new(),
				columns.created_at().to_vec(),
				columns.updated_at().to_vec(),
				columns.time().to_vec(),
			),
		)
	}

	fn stage(&self, columns: &Columns, op: DiffType) {
		let with_implicit = Self::add_implicit_columns(columns, op);
		self.delivery.push(self.subscription_id, with_implicit);
	}
}

impl RawStatefulOperator<EphemeralTransaction> for EphemeralSinkSubscriptionOperator {}

impl Operator<EphemeralTransaction> for EphemeralSinkSubscriptionOperator {
	fn id(&self) -> OperatorId {
		self.plan.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, _txn: &mut EphemeralTransaction, change: Change) -> Result<Change> {
		let plan = self.plan.clone();
		let state = &mut self.state;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => plan.apply_insert(state, post),
				Diff::Update {
					pre,
					post,
					..
				} => plan.apply_update(state, pre, post),
				Diff::Remove {
					pre,
					..
				} => plan.apply_remove(state, pre),
			}
		}

		Ok(Change::from_flow(plan.operator, change.version, Vec::new(), change.changed_at))
	}
}

impl EphemeralSinkPlan {
	fn apply_insert(&self, state: &mut DeliveredState, post: &Columns) {
		let row_count = post.row_count();
		let mut new_indices: Vec<usize> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			if state.rows.insert(post.row_numbers()[row_idx]) {
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
			let pre_rn = pre.row_numbers()[row_idx];
			let post_rn = post.row_numbers()[row_idx];
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
			let pre_rn = pre.row_numbers()[row_idx];
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
