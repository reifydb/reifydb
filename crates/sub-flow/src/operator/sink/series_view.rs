// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::schema::Schema,
	interface::{
		catalog::{flow::FlowNodeId, id::SeriesId, primitive::PrimitiveId, series::TimestampPrecision},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	internal,
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_transaction::interceptor::view::ViewInterceptor;
use reifydb_type::{Result, error::Error, value::row_number::RowNumber};

use super::{coerce_columns, encode_row_at_index};
use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct SinkSeriesViewOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	view: ResolvedView,
	series_id: SeriesId,
	#[allow(dead_code)]
	timestamp_column: Option<String>,
	#[allow(dead_code)]
	precision: TimestampPrecision,
}

impl SinkSeriesViewOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		view: ResolvedView,
		series_id: SeriesId,
		timestamp_column: Option<String>,
		precision: TimestampPrecision,
	) -> Self {
		Self {
			parent,
			node,
			view,
			series_id,
			timestamp_column,
			precision,
		}
	}
}

impl Operator for SinkSeriesViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view_def = self.view.def().clone();
		let schema: Schema = view_def.columns().into();
		let primitive_id = PrimitiveId::series(self.series_id);

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					let coerced = coerce_columns(post, view_def.columns())?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let row_number = coerced.row_numbers[row_idx];
						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &schema, row_number);

						ViewInterceptor::pre_insert(txn, &view_def, row_number, &encoded)?;
						let key = RowKey::encoded(primitive_id, row_number);
						txn.set(&key, encoded.clone())?;
						ViewInterceptor::post_insert(txn, &view_def, row_number, &encoded)?;

						if let Some(log) = txn.testing_mut() {
							let new = Columns::single_row(coerced.iter().map(|col| {
								(col.name().text(), col.data().get_value(row_idx))
							}));
							let mutation_key = format!(
								"views::{}::{}",
								self.view.namespace().name(),
								self.view.name()
							);
							log.record_insert(mutation_key, new);
						}
					}
					let version = txn.version();
					txn.push_view_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view_def.id())),
						version,
						diffs: vec![Diff::Insert {
							post: coerced,
						}],
					});
				}
				Diff::Update {
					..
				} => {
					return Err(Error(internal!(
						"Update diffs are not supported for series-backed views"
					)));
				}
				Diff::Remove {
					..
				} => {
					return Err(Error(internal!(
						"Remove diffs are not supported for series-backed views"
					)));
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, Vec::new()))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		unreachable!()
	}
}
