// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::repeat_n, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	interface::catalog::{id::SubscriptionId, subscription::SubscriptionInspector},
	value::{
		batch::{batch, concat, from_rows},
		column::factory::uint1,
	},
};
use reifydb_value::{
	Result,
	value::system_columns::{is_system_field, user_columns},
};

use crate::store::SubscriptionStore;

const OP_COLUMN: &str = "#op";

pub(super) struct SubscriptionInspectorImpl {
	pub(super) store: Arc<SubscriptionStore>,
}

impl SubscriptionInspectorImpl {
	fn with_op(input: &RecordBatch, ops: Vec<u8>) -> Result<RecordBatch> {
		let mut columns: Vec<(FieldRef, ArrayRef)> =
			user_columns(input).map(|(field, array)| (field.clone(), array.clone())).collect();
		columns.push(uint1(OP_COLUMN, ops));
		columns.extend(input
			.schema_ref()
			.fields()
			.iter()
			.zip(input.columns())
			.filter(|(field, _)| is_system_field(field))
			.map(|(field, array)| (field.clone(), array.clone())));
		batch(columns)
	}
}

impl SubscriptionInspector for SubscriptionInspectorImpl {
	fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.store.active_subscriptions()
	}

	fn inspect(&self, id: SubscriptionId) -> Result<Option<RecordBatch>> {
		let batches = self.store.drain(&id, usize::MAX);
		if batches.is_empty() {
			if !self.store.contains(&id) {
				return Ok(None);
			}
			return Ok(Some(from_rows(&[OP_COLUMN], &[])?));
		}
		if batches.len() == 1 {
			let (op, input) = batches.into_iter().next().unwrap();
			let ops = vec![op.as_u8(); input.num_rows()];
			return Ok(Some(Self::with_op(&input, ops)?));
		}

		let mut all_ops = Vec::new();
		for (op, input) in &batches {
			all_ops.extend(repeat_n(op.as_u8(), input.num_rows()));
		}
		let inputs: Vec<RecordBatch> = batches.into_iter().map(|(_, input)| input).collect();
		let merged = concat(&inputs)?;
		Ok(Some(Self::with_op(&merged, all_ops)?))
	}
}
