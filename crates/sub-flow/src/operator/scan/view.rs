// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	encoded::shape::{RowShape, RowShapeField},
	interface::{
		catalog::{flow::FlowNodeId, shape::ShapeId, view::View},
		change::{Change, ChangeOrigin, Diff},
	},
	key::row::RowKey,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_type::{
	Result,
	fragment::Fragment,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{Operator, operator::sink::decode_dictionary_columns, transaction::FlowTransaction};

enum OverlayRow<'a> {
	Present(&'a Columns, usize),
	Removed,
}

fn build_view_overlay<'a>(overlay: &'a [Change], view_id: u64) -> HashMap<RowNumber, OverlayRow<'a>> {
	let mut map: HashMap<RowNumber, OverlayRow<'a>> = HashMap::new();
	for change in overlay {
		let ChangeOrigin::Shape(ShapeId::View(id)) = change.origin else {
			continue;
		};
		if id.0 != view_id {
			continue;
		}
		for diff in &change.diffs {
			match diff {
				Diff::Insert {
					post,
				} => {
					for (idx, rn) in post.row_numbers.iter().enumerate() {
						map.insert(*rn, OverlayRow::Present(post, idx));
					}
				}
				Diff::Update {
					post,
					..
				} => {
					for (idx, rn) in post.row_numbers.iter().enumerate() {
						map.insert(*rn, OverlayRow::Present(post, idx));
					}
				}
				Diff::Remove {
					pre,
				} => {
					for rn in pre.row_numbers.iter() {
						map.insert(*rn, OverlayRow::Removed);
					}
				}
			}
		}
	}
	map
}

pub struct PrimitiveViewOperator {
	node: FlowNodeId,
	view: View,
}

impl PrimitiveViewOperator {
	pub fn new(node: FlowNodeId, view: View) -> Self {
		Self {
			node,
			view,
		}
	}
}

impl Operator for PrimitiveViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
				} => {
					let mut decoded = post;
					decode_dictionary_columns(Arc::make_mut(&mut decoded), txn)?;
					Diff::insert_arc(decoded)
				}
				Diff::Update {
					pre,
					post,
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(Arc::make_mut(&mut decoded_pre), txn)?;
					decode_dictionary_columns(Arc::make_mut(&mut decoded_post), txn)?;
					Diff::update_arc(decoded_pre, decoded_post)
				}
				Diff::Remove {
					pre,
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(Arc::make_mut(&mut decoded), txn)?;
					Diff::remove_arc(decoded)
				}
			});
		}
		Ok(Change::from_flow(self.node, change.version, decoded_diffs, change.changed_at))
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> Result<Columns> {
		if rows.is_empty() {
			return Ok(Columns::from_catalog_columns(self.view.columns()));
		}

		let shape: RowShape = self.view.columns().into();
		let fields = shape.fields();

		let overlay_arc = txn.view_overlay();
		let overlay = overlay_arc
			.as_deref()
			.map(|o| build_view_overlay(o.as_slice(), self.view.id().0))
			.unwrap_or_default();

		let mut columns_vec = self.allocate_pull_columns(fields, rows.len());
		let mut row_numbers = Vec::with_capacity(rows.len());
		let mut created_at = Vec::with_capacity(rows.len());
		let mut updated_at = Vec::with_capacity(rows.len());

		for row_num in rows {
			if self.try_push_overlay_row(
				*row_num,
				&overlay,
				&mut columns_vec,
				&mut row_numbers,
				&mut created_at,
				&mut updated_at,
			) {
				continue;
			}
			self.try_push_storage_row(
				txn,
				*row_num,
				&shape,
				fields,
				&mut columns_vec,
				&mut row_numbers,
				&mut created_at,
				&mut updated_at,
			)?;
		}

		if row_numbers.is_empty() {
			Ok(Columns::from_catalog_columns(self.view.columns()))
		} else {
			Ok(Columns::with_system_columns(columns_vec, row_numbers, created_at, updated_at))
		}
	}
}

impl PrimitiveViewOperator {
	#[inline]
	fn allocate_pull_columns(&self, fields: &[RowShapeField], capacity: usize) -> Vec<ColumnWithName> {
		let mut columns_vec: Vec<ColumnWithName> = Vec::with_capacity(fields.len());
		for field in fields.iter() {
			columns_vec.push(ColumnWithName {
				name: Fragment::internal(&field.name),
				data: ColumnBuffer::with_capacity(field.constraint.get_type(), capacity),
			});
		}
		columns_vec
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn try_push_overlay_row(
		&self,
		row_num: RowNumber,
		overlay: &HashMap<RowNumber, OverlayRow<'_>>,
		columns_vec: &mut [ColumnWithName],
		row_numbers: &mut Vec<RowNumber>,
		created_at: &mut Vec<DateTime>,
		updated_at: &mut Vec<DateTime>,
	) -> bool {
		match overlay.get(&row_num) {
			Some(OverlayRow::Removed) => true,
			Some(OverlayRow::Present(src, idx)) => {
				row_numbers.push(row_num);
				created_at.push(src.created_at.get(*idx).copied().unwrap_or_default());
				updated_at.push(src.updated_at.get(*idx).copied().unwrap_or_default());
				for (i, col) in src.iter().enumerate() {
					if i < columns_vec.len() {
						columns_vec[i].data.push_value(col.data().get_value(*idx));
					}
				}
				true
			}
			None => false,
		}
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn try_push_storage_row(
		&self,
		txn: &mut FlowTransaction,
		row_num: RowNumber,
		shape: &RowShape,
		fields: &[RowShapeField],
		columns_vec: &mut [ColumnWithName],
		row_numbers: &mut Vec<RowNumber>,
		created_at: &mut Vec<DateTime>,
		updated_at: &mut Vec<DateTime>,
	) -> Result<()> {
		let key = RowKey::encoded(self.view.underlying_id(), row_num);
		if let Some(encoded) = txn.get(&key)? {
			row_numbers.push(row_num);
			created_at.push(DateTime::from_nanos(encoded.created_at_nanos()));
			updated_at.push(DateTime::from_nanos(encoded.updated_at_nanos()));
			for (i, _field) in fields.iter().enumerate() {
				let value = shape.get_value(&encoded, i);
				columns_vec[i].data.push_value(value);
			}
		}
		Ok(())
	}
}
