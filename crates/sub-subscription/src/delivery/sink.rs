// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, iter::once, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_core::{
	interface::{
		catalog::{flow::OperatorId, id::SubscriptionId},
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	metrics::heap::HeapSize,
	value::batch::take_rows,
};
use reifydb_flow_async::operator::{HostOperator, host::HostContext};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result, reifydb_assertions,
	value::{
		diff_type::DiffType,
		row_number::RowNumber,
		system_columns::{SystemColumn, keep_system_columns, require_row_numbers, system_column},
	},
};

use crate::delivery::DeliveryBuffer;

#[operator_state]
#[derive(Debug, Clone, Default, HeapSize)]
struct DeliveredState {
	rows: BTreeSet<RowNumber>,
}

pub struct EphemeralSinkPlan {
	operator: OperatorId,
	subscription_id: SubscriptionId,
	keep: Vec<SystemColumn>,
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
		named_system_columns: &[SystemColumn],
		delivery: Arc<DeliveryBuffer>,
	) -> Self {
		Self {
			plan: Arc::new(EphemeralSinkPlan {
				operator,
				subscription_id,
				keep: once(SystemColumn::RowNumbers)
					.chain(named_system_columns.iter().copied())
					.collect(),
				delivery,
			}),
			state: DeliveredState::default(),
		}
	}
}

impl EphemeralSinkPlan {
	fn stage(&self, batch: &RecordBatch, op: DiffType) -> Result<()> {
		let batch = keep_system_columns(batch, &self.keep)?;
		reifydb_assertions! {
			assert!(
				batch.num_rows() == 0 || system_column(&batch, SystemColumn::RowNumbers).is_some(),
				"a staged change batch carries no row numbers for {} rows, so a subscriber could not \
				 identify which entity changed and would leak or drop rows",
				batch.num_rows()
			);
		}
		self.delivery.push(self.subscription_id, op, batch);
		Ok(())
	}
}

fn staged_row_numbers(batch: &RecordBatch) -> Result<&[RowNumber]> {
	if batch.num_rows() == 0 {
		return Ok(&[]);
	}
	require_row_numbers(batch)
}

impl HostOperator for EphemeralSinkSubscriptionOperator {
	fn id(&self) -> OperatorId {
		self.plan.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let plan = self.plan.clone();
		let state = &mut self.state;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => plan.apply_insert(state, post)?,
				Diff::Update {
					pre,
					post,
					..
				} => plan.apply_update(state, pre, post)?,
				Diff::Remove {
					pre,
					..
				} => plan.apply_remove(state, pre)?,
			}
		}

		Ok(Change::from_flow(plan.operator, change.version, Vec::new(), change.changed_at))
	}
}

impl EphemeralSinkPlan {
	fn apply_insert(&self, state: &mut DeliveredState, post: &RecordBatch) -> Result<()> {
		let row_count = post.num_rows();
		let post_row_numbers = staged_row_numbers(post)?;
		let mut new_indices: Vec<usize> = Vec::with_capacity(row_count);
		for (row_idx, &row_number) in post_row_numbers.iter().enumerate().take(row_count) {
			if state.rows.insert(row_number) {
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
			self.stage(post, DiffType::Insert)?;
		} else if !new_indices.is_empty() {
			let sub_post = take_rows(post, &new_indices)?;
			self.stage(&sub_post, DiffType::Insert)?;
		}
		Ok(())
	}

	fn apply_update(&self, state: &mut DeliveredState, pre: &RecordBatch, post: &RecordBatch) -> Result<()> {
		let row_count = post.num_rows();
		let pre_row_numbers = staged_row_numbers(pre)?;
		let post_row_numbers = staged_row_numbers(post)?;
		let mut update_indices: Vec<usize> = Vec::new();
		let mut insert_indices: Vec<usize> = Vec::new();
		for row_idx in 0..row_count {
			let pre_rn = pre_row_numbers[row_idx];
			let post_rn = post_row_numbers[row_idx];
			reifydb_assertions! {
				assert!(
					pre_rn == post_rn,
					"an update renumbered a row from {} to {}, but a subscriber keys its state on the \
					 row number and would keep the old row forever while treating the new one as an \
					 unrelated insert",
					pre_rn.value(),
					post_rn.value()
				);
			}
			if state.rows.contains(&pre_rn) {
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
			let sub_post = take_rows(post, &update_indices)?;
			self.stage(&sub_post, DiffType::Update)?;
		}
		if !insert_indices.is_empty() {
			let sub_post = take_rows(post, &insert_indices)?;
			self.stage(&sub_post, DiffType::Insert)?;
		}
		Ok(())
	}

	fn apply_remove(&self, state: &mut DeliveredState, pre: &RecordBatch) -> Result<()> {
		let row_count = pre.num_rows();
		let pre_row_numbers = staged_row_numbers(pre)?;
		let mut remove_indices: Vec<usize> = Vec::new();
		for (row_idx, pre_rn) in pre_row_numbers.iter().enumerate().take(row_count) {
			if state.rows.remove(pre_rn) {
				remove_indices.push(row_idx);
			}
		}
		if !remove_indices.is_empty() {
			let sub_pre = take_rows(pre, &remove_indices)?;
			self.stage(&sub_pre, DiffType::Remove)?;
		}
		Ok(())
	}
}
