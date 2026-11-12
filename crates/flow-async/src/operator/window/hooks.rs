// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::change::Diff, internal_err};
use reifydb_flow::aggregate::SlotKind;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber},
};

use crate::{
	operator::{
		aggregation::{
			accumulator::RowAccumulator,
			core::{Aggregation, EmitRow},
		},
		state::store,
	},
	window::{
		coord::DateTimeCoord,
		driver::hooks::{WindowEmits, WindowHooks},
		span::WindowSpan,
	},
};

pub struct BuiltinRow {
	group: Hash128,
	span: WindowSpan<DateTimeCoord>,
	values: Vec<Value>,
}

pub struct BuiltinHooks<'a> {
	core: &'a mut Aggregation,
	kinds: &'a [SlotKind],
	immutable: Option<Duration>,
	group_values: HashMap<Hash128, Vec<Value>>,
	changed_at: DateTime,
}

impl<'a> BuiltinHooks<'a> {
	pub fn new(
		core: &'a mut Aggregation,
		kinds: &'a [SlotKind],
		immutable: Option<Duration>,
		group_values: HashMap<Hash128, Vec<Value>>,
		changed_at: DateTime,
	) -> Self {
		Self {
			core,
			kinds,
			immutable,
			group_values,
			changed_at,
		}
	}

	fn emit_row(&self, row_number: RowNumber, time: DateTime, row: BuiltinRow) -> Result<EmitRow> {
		let Some(group_values) = self.group_values.get(&row.group).cloned() else {
			return internal_err!("window result for group {:?} has no routed group values", row.group);
		};
		Ok(EmitRow {
			group_values,
			slot_values: row.values,
			row_number,
			span: Some(row.span),
			time: Some(time),
		})
	}
}

impl WindowHooks for BuiltinHooks<'_> {
	type Group = Hash128;
	type Coord = DateTimeCoord;
	type Accumulator = RowAccumulator;
	type Row = BuiltinRow;
	type Out = Vec<Diff>;

	fn new_accumulator(&self) -> RowAccumulator {
		RowAccumulator::new(self.kinds, self.immutable)
	}

	fn group_identity(&self, group: &Hash128) -> (Hash128, EncodedKey) {
		(*group, store::empty_key())
	}

	fn build(&self, group: &Hash128, span: WindowSpan<DateTimeCoord>, value: &Vec<Value>) -> Option<BuiltinRow> {
		Some(BuiltinRow {
			group: *group,
			span,
			values: value.clone(),
		})
	}

	fn emit(&mut self, emits: WindowEmits<BuiltinRow>) -> Result<Vec<Diff>> {
		let mut inserts = Vec::with_capacity(emits.inserts.len());
		for (row_number, time, row) in emits.inserts {
			inserts.push(self.emit_row(row_number, time, row)?);
		}
		let mut updates = Vec::with_capacity(emits.updates.len());
		for (row_number, pre_time, post_time, pre, post) in emits.updates {
			updates.push((
				self.emit_row(row_number, pre_time, pre)?,
				self.emit_row(row_number, post_time, post)?,
			));
		}
		let mut removes = Vec::with_capacity(emits.removes.len());
		for (row_number, time, row) in emits.removes {
			removes.push(self.emit_row(row_number, time, row)?);
		}
		self.core.emit_diffs(inserts, updates, removes, self.changed_at)
	}
}
