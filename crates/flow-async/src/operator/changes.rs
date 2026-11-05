// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	internal_err,
	operator_with::{ChangesWith, DeleteTo, InsertTo, UpdateTo},
	value::batch::take_rows,
};
use reifydb_value::{
	Result,
	value::{
		column_view::ColumnView,
		container::temporal_array::datetime_array,
		datetime::DateTime,
		system_columns::{SystemColumn, column_view, with_system_column},
	},
};

use crate::operator::{HostOperator, host::HostContext, state_access::RowNumberReserve};

const APART_FROM_SOURCE_NUMBERS: u64 = 1 << 63;

pub struct ChangesOperator {
	operator: OperatorId,
	parent_schema: Option<SchemaRef>,
	with: ChangesWith,
}

impl ChangesOperator {
	pub fn new(operator: OperatorId, parent_schema: Option<SchemaRef>, with: ChangesWith) -> Self {
		Self {
			operator,
			parent_schema,
			with,
		}
	}

	fn numbered(
		&self,
		host: &mut dyn HostContext,
		reserve: &mut RowNumberReserve,
		rows: RecordBatch,
	) -> Result<RecordBatch> {
		let apart = match self.with.update {
			UpdateTo::Update => APART_FROM_SOURCE_NUMBERS,
			UpdateTo::Insert | UpdateTo::Drop => 0,
		};
		let mut numbers = Vec::with_capacity(rows.num_rows());
		for _ in 0..rows.num_rows() {
			numbers.push(reserve.take(host)?.0 | apart);
		}
		with_system_column(rows, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(numbers)))
	}

	fn moved(&self, pre: &RecordBatch, post: &RecordBatch) -> Result<Vec<usize>> {
		let mut views = Vec::with_capacity(self.with.update_on.len());
		for name in &self.with.update_on {
			let (Some(before), Some(after)) = (column_view(pre, name)?, column_view(post, name)?) else {
				return internal_err!("changes has no column {} to compare", name);
			};
			views.push((before, after));
		}
		Ok((0..post.num_rows())
			.filter(|&row| views.iter().any(|(before, after)| differs(before, after, row)))
			.collect())
	}

	fn update_rows(&self, pre: &RecordBatch, post: RecordBatch) -> Result<RecordBatch> {
		if self.with.update_on.is_empty() {
			return Ok(post);
		}
		let rows = self.moved(pre, &post)?;
		if rows.len() == post.num_rows() {
			return Ok(post);
		}
		take_rows(&post, &rows)
	}
}

fn differs(before: &ColumnView<'_>, after: &ColumnView<'_>, row: usize) -> bool {
	before.get_value(row) != after.get_value(row)
}

fn timed(rows: RecordBatch, at: DateTime) -> Result<RecordBatch> {
	let times = datetime_array(vec![at; rows.num_rows()]);
	with_system_column(rows, SystemColumn::Time, Arc::new(times))
}

impl HostOperator for ChangesOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let mut reserve = RowNumberReserve::default();
		let mut diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			match diff {
				Diff::Insert {
					post,
					..
				} if self.with.insert == InsertTo::Insert => {
					let rows = match self.with.renumbers_inserts() {
						true => self.numbered(host, &mut reserve, post)?,
						false => post,
					};
					diffs.push(Diff::insert(rows));
				}
				Diff::Insert {
					..
				} => {}
				update @ Diff::Update {
					..
				} if self.with.update == UpdateTo::Update => diffs.push(update),
				Diff::Update {
					pre,
					post,
					..
				} if self.with.update == UpdateTo::Insert => {
					let rows = self.update_rows(&pre, post)?;
					if rows.num_rows() > 0 {
						diffs.push(Diff::insert(self.numbered(host, &mut reserve, rows)?));
					}
				}
				Diff::Update {
					..
				} => {}
				remove @ Diff::Remove {
					..
				} if self.with.delete == DeleteTo::Delete => diffs.push(remove),
				Diff::Remove {
					pre,
					..
				} if self.with.delete == DeleteTo::Insert => {
					let rows = timed(pre, change.changed_at)?;
					diffs.push(Diff::insert(self.numbered(host, &mut reserve, rows)?));
				}
				Diff::Remove {
					..
				} => {}
			}
		}
		reserve.commit(host)?;
		Ok(Change::from_flow(self.operator, change.version, diffs, change.changed_at))
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		self.parent_schema.clone()
	}
}
