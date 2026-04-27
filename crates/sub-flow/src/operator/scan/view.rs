// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	encoded::shape::RowShape,
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

/// Final state of a single row according to the in-transaction view overlay.
///
/// `Present(columns, idx)` - the row exists with data at `columns[idx]`.
/// `Removed` - the row was removed in this transaction and should be absent
/// from the pull result.
enum OverlayRow<'a> {
	Present(&'a Columns, usize),
	Removed,
}

/// Build a per-row lookup of the overlay's effect on the given view.
///
/// Walks `overlay` in order, collapsing multiple diffs for the same row_number
/// so the final entry reflects the latest state (later diffs override earlier
/// ones, Insert/Update write a Present entry, Remove writes a Removed entry).
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

		// Build the in-transaction overlay for this view (sibling views' outputs
		// produced earlier in the same pre-commit). Empty for Deferred / Ephemeral
		// transactions - those read everything from committed storage.
		// Hold the Arc in a local so the overlay HashMap (which borrows from it)
		// stays alive across the subsequent mutable borrow for `txn.get`.
		let overlay_arc = txn.view_overlay();
		let overlay = overlay_arc
			.as_deref()
			.map(|o| build_view_overlay(o.as_slice(), self.view.id().0))
			.unwrap_or_default();

		// Pre-allocate columns with capacity
		let mut columns_vec: Vec<ColumnWithName> = Vec::with_capacity(fields.len());
		for field in fields.iter() {
			columns_vec.push(ColumnWithName {
				name: Fragment::internal(&field.name),
				data: ColumnBuffer::with_capacity(field.constraint.get_type(), rows.len()),
			});
		}
		let mut row_numbers = Vec::with_capacity(rows.len());
		let mut created_at = Vec::with_capacity(rows.len());
		let mut updated_at = Vec::with_capacity(rows.len());

		for row_num in rows {
			// Overlay takes precedence over read_version storage: it reflects
			// writes performed in this transaction by sibling views.
			match overlay.get(row_num) {
				Some(OverlayRow::Removed) => continue,
				Some(OverlayRow::Present(src, idx)) => {
					row_numbers.push(*row_num);
					let src_created_at = src.created_at.get(*idx).copied().unwrap_or_default();
					let src_updated_at = src.updated_at.get(*idx).copied().unwrap_or_default();
					created_at.push(src_created_at);
					updated_at.push(src_updated_at);
					for (i, col) in src.iter().enumerate() {
						if i < columns_vec.len() {
							columns_vec[i].data.push_value(col.data().get_value(*idx));
						}
					}
					continue;
				}
				None => {}
			}

			let key = RowKey::encoded(self.view.underlying_id(), *row_num);
			if let Some(encoded) = txn.get(&key)? {
				row_numbers.push(*row_num);
				created_at.push(DateTime::from_nanos(encoded.created_at_nanos()));
				updated_at.push(DateTime::from_nanos(encoded.updated_at_nanos()));
				// Decode each column value directly
				for (i, _field) in fields.iter().enumerate() {
					let value = shape.get_value(&encoded, i);
					columns_vec[i].data.push_value(value);
				}
			}
		}

		if row_numbers.is_empty() {
			Ok(Columns::from_catalog_columns(self.view.columns()))
		} else {
			Ok(Columns::with_system_columns(columns_vec, row_numbers, created_at, updated_at))
		}
	}
}
