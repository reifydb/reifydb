// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_catalog::store::subscription::subscription_delta;
use reifydb_core::{
	interface::{FlowNodeId, ResolvedSubscription},
	key::SubscriptionDeltaKey,
	value::{
		column::Columns,
		encoded::{EncodedValues, EncodedValuesNamedLayout},
	},
};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_sdk::{FlowChange, FlowDiff};
use reifydb_type::{RowNumber, Value};

use super::coerce_subscription_columns;
use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct SinkSubscriptionOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	subscription: ResolvedSubscription,
}

impl SinkSubscriptionOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, subscription: ResolvedSubscription) -> Self {
		Self {
			parent,
			node,
			subscription,
		}
	}

	/// Encode row values into a blob for storage
	fn encode_row_values(columns: &Columns, row_idx: usize, layout: &EncodedValuesNamedLayout) -> Vec<u8> {
		let values: Vec<Value> = columns.iter().map(|c| c.data().get_value(row_idx)).collect();
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, &values);
		encoded.to_vec()
	}

	/// Create a delta entry with op, pre, and post blobs
	fn create_delta_entry(op: u8, pre: Option<Vec<u8>>, post: Option<Vec<u8>>) -> EncodedValues {
		let layout = &*subscription_delta::LAYOUT;
		let mut encoded = layout.allocate();

		// Set op
		layout.set_u8(&mut encoded, subscription_delta::OP, op);

		// Set pre (null for Insert)
		match pre {
			Some(bytes) => layout.set_blob(&mut encoded, subscription_delta::PRE, &bytes.into()),
			None => layout.set_undefined(&mut encoded, subscription_delta::PRE),
		}

		// Set post (null for Delete)
		match post {
			Some(bytes) => layout.set_blob(&mut encoded, subscription_delta::POST, &bytes.into()),
			None => layout.set_undefined(&mut encoded, subscription_delta::POST),
		}

		encoded
	}
}

#[async_trait]
impl Operator for SinkSubscriptionOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	async fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		let subscription_def = self.subscription.def().clone();
		let layout: EncodedValuesNamedLayout = (&subscription_def).into();

		// Track sequence number for deltas within this change
		let mut sequence: u16 = 0;

		for diff in change.diffs.iter() {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					// Coerce columns to match subscription schema types
					let coerced = coerce_subscription_columns(post, self.subscription.columns())?;
					let row_count = coerced.row_count();

					for row_idx in 0..row_count {
						let post_bytes = Self::encode_row_values(&coerced, row_idx, &layout);
						let delta = Self::create_delta_entry(
							subscription_delta::OP_INSERT,
							None,
							Some(post_bytes),
						);

						let key = SubscriptionDeltaKey::encoded(
							subscription_def.id,
							change.version,
							sequence,
						);
						txn.set(&key, delta)?;

						sequence = sequence.wrapping_add(1);
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					// Coerce columns to match subscription schema types
					let coerced_pre =
						coerce_subscription_columns(pre, self.subscription.columns())?;
					let coerced_post =
						coerce_subscription_columns(post, self.subscription.columns())?;
					let row_count = coerced_post.row_count();

					for row_idx in 0..row_count {
						let pre_bytes = Self::encode_row_values(&coerced_pre, row_idx, &layout);
						let post_bytes =
							Self::encode_row_values(&coerced_post, row_idx, &layout);
						let delta = Self::create_delta_entry(
							subscription_delta::OP_UPDATE,
							Some(pre_bytes),
							Some(post_bytes),
						);

						let key = SubscriptionDeltaKey::encoded(
							subscription_def.id,
							change.version,
							sequence,
						);
						txn.set(&key, delta)?;

						sequence = sequence.wrapping_add(1);
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					// Coerce columns to match subscription schema types
					let coerced = coerce_subscription_columns(pre, self.subscription.columns())?;
					let row_count = coerced.row_count();

					for row_idx in 0..row_count {
						let pre_bytes = Self::encode_row_values(&coerced, row_idx, &layout);
						let delta = Self::create_delta_entry(
							subscription_delta::OP_DELETE,
							Some(pre_bytes),
							None,
						);

						let key = SubscriptionDeltaKey::encoded(
							subscription_def.id,
							change.version,
							sequence,
						);
						txn.set(&key, delta)?;

						sequence = sequence.wrapping_add(1);
					}
				}
			}
		}

		Ok(FlowChange::internal(self.node, change.version, Vec::new()))
	}

	async fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> crate::Result<Columns> {
		unreachable!()
	}
}
