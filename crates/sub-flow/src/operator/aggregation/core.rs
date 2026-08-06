// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::UnsafeCell, sync::Arc};

use postcard::to_stdvec;
use reifydb_codec::{
	encoded::shape::{RowShape, RowShapeField},
	key::{encoded::EncodedKey, serializer::KeySerializer},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	metrics::heap::{StateCompleteness, StateMemory},
	row::Row,
	state::{budget::OperatorStateBudgetHandle, cache::StateCache, store::StateStore},
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	flow::aggregate::{AggregateContext, SlotArg, SlotKind, rewrite_aggregates, synthetic_aggregate_column_name},
};
use reifydb_flow::window::{
	engine::tumbling::TumblingEngine,
	meta::{EngineMeta, EngineMetaKey},
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::{Expression, name::display_label};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::{Hash128, xxh3_128},
	value::{Value, datetime::DateTime, row_number::RowNumber, value_type::ValueType},
};

use crate::{
	context::FlowContext,
	error::FlowStateError,
	operator::{OperatorCell, aggregation::accumulator::RowAccumulator},
};

#[derive(Clone, Debug)]
pub enum SlotInput {
	Star,
	Column(String),
	Expr(usize),
}

#[inline]
fn build_aggregation_shape(names: &[String], types: &[ValueType]) -> RowShape {
	let fields: Vec<RowShapeField> = names
		.iter()
		.zip(types.iter())
		.map(|(name, ty)| RowShapeField::unconstrained(name.clone(), ty.clone()))
		.collect();
	RowShape::new(fields)
}

pub struct Aggregation {
	pub operator: OperatorId,
	pub parent: OperatorCell,
	pub compiled_group_by: Vec<CompiledExpr>,
	pub group_names: Vec<String>,
	pub aggregate_output_names: Vec<String>,

	pub slot_kinds: Option<Vec<SlotKind>>,

	pub slot_inputs: Vec<SlotInput>,

	pub compiled_slot_args: Vec<CompiledExpr>,

	pub compiled_outputs: Vec<CompiledExpr>,

	pub routines: Routines,
	pub runtime_context: RuntimeContext,
	tumbling_engine: UnsafeCell<Option<Box<TumblingEngine<Hash128, DateTime, RowAccumulator>>>>,
	engine_meta: UnsafeCell<Option<StateCache<EngineMetaKey, EngineMeta>>>,
	pub ctx: Arc<FlowContext>,
}

impl Aggregation {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		operator: OperatorId,
		parent: OperatorCell,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		context: AggregateContext,
		ctx: Arc<FlowContext>,
	) -> Self {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};

		let compiled_group_by: Vec<CompiledExpr> = group_by
			.iter()
			.map(|e| compile_expression(&compile_ctx, e).expect("Failed to compile group_by expression"))
			.collect();

		let aggregate_output_names: Vec<String> =
			aggregations.iter().map(|e| display_label(e).text().to_string()).collect();

		let mut slots: Vec<(SlotKind, SlotArg)> = Vec::new();
		let mut rewritten_outputs: Vec<Expression> = Vec::new();
		let mut all_representable = !aggregations.is_empty();
		for aggregate in &aggregations {
			let mut expr = aggregate.clone();
			if rewrite_aggregates(&routines, &mut expr, &mut slots, context) {
				rewritten_outputs.push(expr);
			} else {
				all_representable = false;
				break;
			}
		}
		let (slot_kinds, slot_inputs, compiled_slot_args, compiled_outputs) = if all_representable {
			let mut kinds = Vec::with_capacity(slots.len());
			let mut inputs = Vec::with_capacity(slots.len());
			let mut compiled_args = Vec::new();
			for (kind, arg) in slots {
				kinds.push(kind);
				inputs.push(match arg {
					SlotArg::Star => SlotInput::Star,
					SlotArg::Column(name) => SlotInput::Column(name),
					SlotArg::Expr(expr) => {
						let idx = compiled_args.len();
						compiled_args.push(compile_expression(&compile_ctx, &expr)
							.expect("Failed to compile aggregation argument expression"));
						SlotInput::Expr(idx)
					}
				});
			}
			let outputs: Vec<CompiledExpr> = rewritten_outputs
				.iter()
				.map(|e| {
					compile_expression(&compile_ctx, e)
						.expect("Failed to compile rewritten output expression")
				})
				.collect();
			(Some(kinds), inputs, compiled_args, outputs)
		} else {
			(None, Vec::new(), Vec::new(), Vec::new())
		};
		let group_names: Vec<String> = group_by.iter().map(|e| display_label(e).text().to_string()).collect();

		Self {
			operator,
			parent,
			compiled_group_by,
			group_names,
			aggregate_output_names,
			slot_kinds,
			slot_inputs,
			compiled_slot_args,
			compiled_outputs,
			routines,
			runtime_context,
			tumbling_engine: UnsafeCell::new(None),
			engine_meta: UnsafeCell::new(None),
			ctx,
		}
	}

	#[allow(clippy::mut_from_ref)]
	pub(crate) fn tumbling_engine_slot(
		&self,
	) -> &mut Option<Box<TumblingEngine<Hash128, DateTime, RowAccumulator>>> {
		// SAFETY: one actor owns this operator and apply/tick never re-enter, so no other borrow
		// of the UnsafeCell is live while this &mut exists.
		unsafe { &mut *self.tumbling_engine.get() }
	}

	pub(crate) fn engine_meta_open(&self, budget: OperatorStateBudgetHandle) {
		// SAFETY: one actor owns this operator and apply/tick run single-threaded, so no other
		// borrow of the UnsafeCell is live while this &mut exists.
		let slot = unsafe { &mut *self.engine_meta.get() };
		if slot.is_none() {
			*slot = Some(StateCache::new(budget));
		}
	}

	#[allow(clippy::mut_from_ref)]
	pub(crate) fn engine_meta(&self) -> &mut StateCache<EngineMetaKey, EngineMeta> {
		// SAFETY: single-threaded per actor, so no other borrow of the UnsafeCell is live; the
		// slot is non-none because engine_meta_open runs at the apply/tick entry.
		unsafe { (*self.engine_meta.get()).as_mut().expect("engine_meta opened at apply/tick entry") }
	}

	pub(crate) fn engine_meta_flush<S: StateStore>(&self, store: &mut S) -> Result<()> {
		// SAFETY: single-threaded per actor; no aliasing &mut engine_meta borrow is live here.
		if let Some(cache) = unsafe { &mut *self.engine_meta.get() } {
			cache.flush(store)?;
		}
		Ok(())
	}

	pub(crate) fn engine_meta_sample_parts(
		&self,
	) -> Option<(StateMemory, StateMemory, StateMemory, StateCompleteness)> {
		// SAFETY: single-threaded per actor and sample runs sequentially with apply/tick, so no
		// aliasing borrow of the UnsafeCell is live.
		let cache = unsafe { (*self.engine_meta.get()).as_ref() }?;
		Some((
			cache.approximate_memory(),
			cache.dirty_memory(),
			cache.membership_memory(),
			cache.completeness(),
		))
	}

	pub fn create_window_key(&self, group_hash: Hash128, window_id: u64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"win:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(window_id);
		serializer.finish()
	}

	pub(crate) fn reset_engine_caches(&self) {
		// SAFETY: compaction runs on the tick path, sequential with apply on the same actor, so
		// no other borrow of either UnsafeCell is live while these &mut exist.
		unsafe {
			*self.tumbling_engine.get() = None;
			*self.engine_meta.get() = None;
		}
	}

	pub fn compute_groups(&self, columns: &Columns) -> Result<Vec<(Hash128, Vec<Value>)>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}
		if self.compiled_group_by.is_empty() {
			return Ok(vec![(Hash128::from(0u128), Vec::new()); row_count]);
		}

		let session = self.eval_session();
		let exec_ctx = session.with_eval(columns.clone(), row_count);
		let mut group_columns: Vec<ColumnWithName> = Vec::new();
		for compiled_expr in &self.compiled_group_by {
			group_columns.push(compiled_expr.execute(&exec_ctx)?);
		}

		let mut out = Vec::with_capacity(row_count);
		let mut buf = Vec::with_capacity(128);
		for row_idx in 0..row_count {
			buf.clear();
			let mut values = Vec::with_capacity(group_columns.len());
			for col in &group_columns {
				let value = col.data().get_value(row_idx);
				let bytes = to_stdvec(&value).map_err(|e| {
					Error::from(FlowStateError::Encode {
						state: "group-by value",
						cause: e.to_string(),
					})
				})?;
				buf.extend_from_slice(&bytes);
				values.push(value);
			}
			out.push((xxh3_128(&buf), values));
		}
		Ok(out)
	}

	pub fn evaluate_slot_inputs(&self, columns: &Columns) -> Result<Vec<ColumnWithName>> {
		if self.compiled_slot_args.is_empty() {
			return Ok(Vec::new());
		}
		let row_count = columns.row_count();
		let session = self.eval_session();
		let exec_ctx = session.with_eval(columns.clone(), row_count);
		let mut out = Vec::with_capacity(self.compiled_slot_args.len());
		for compiled in &self.compiled_slot_args {
			out.push(compiled.execute(&exec_ctx)?);
		}
		Ok(out)
	}

	pub fn build_contribution(
		&self,
		columns: &Columns,
		slot_cols: &[ColumnWithName],
		row_idx: usize,
	) -> Vec<Option<Value>> {
		self.slot_inputs
			.iter()
			.map(|input| match input {
				SlotInput::Star => None,
				SlotInput::Column(name) => columns.column(name).map(|c| c.data().get_value(row_idx)),
				SlotInput::Expr(idx) => Some(slot_cols[*idx].data().get_value(row_idx)),
			})
			.collect()
	}

	pub fn compute_outputs(&self, slot_values: &[Value]) -> Result<Vec<Value>> {
		if self.compiled_outputs.is_empty() {
			return Ok(slot_values.to_vec());
		}
		let names: Vec<String> = (0..slot_values.len()).map(synthetic_aggregate_column_name).collect();
		let types: Vec<_> = slot_values.iter().map(Value::get_type).collect();
		let layout = build_aggregation_shape(&names, &types);
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, slot_values);
		let row = Row {
			number: RowNumber(0),
			encoded: encoded.freeze(),
			shape: layout,
		};
		let columns = Columns::from_row(&row);
		let session = self.eval_session();
		let exec_ctx = session.with_eval(columns, 1);
		let mut out = Vec::with_capacity(self.compiled_outputs.len());
		for compiled in &self.compiled_outputs {
			out.push(compiled.execute(&exec_ctx)?.data().get_value(0));
		}
		Ok(out)
	}

	pub fn build_engine_row(
		&self,
		group_values: &[Value],
		slot_values: &[Value],
		row_number: RowNumber,
		ts: DateTime,
		time: DateTime,
	) -> Result<Row> {
		let aggregate_values = self.compute_outputs(slot_values)?;
		let mut values = Vec::with_capacity(group_values.len() + aggregate_values.len());
		let mut names = Vec::with_capacity(group_values.len() + aggregate_values.len());
		let mut types = Vec::with_capacity(group_values.len() + aggregate_values.len());
		for (value, name) in group_values.iter().zip(self.group_names.iter()) {
			types.push(value.get_type());
			values.push(value.clone());
			names.push(name.clone());
		}
		for (value, name) in aggregate_values.iter().zip(self.aggregate_output_names.iter()) {
			types.push(value.get_type());
			values.push(value.clone());
			names.push(name.clone());
		}
		let layout = build_aggregation_shape(&names, &types);
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, &values);
		encoded.set_timestamps(ts, ts);
		encoded.set_time(time);
		Ok(Row {
			number: row_number,
			encoded: encoded.freeze(),
			shape: layout,
		})
	}

	fn eval_session(&self) -> EvalContext<'_> {
		EvalContext {
			params: &self.ctx.params,
			symbols: &self.ctx.symbols,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: self.ctx.identity,
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		}
	}
}
