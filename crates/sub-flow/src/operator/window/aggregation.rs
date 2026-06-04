// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use postcard::to_stdvec;
use reifydb_core::{
	encoded::key::EncodedKey,
	interface::{
		catalog::flow::FlowNodeId,
		identifier::{ColumnIdentifier, ColumnShape},
	},
	internal,
	row::Row,
	util::encoding::keycode::serializer::KeySerializer,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_engine::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::{ColumnExpression, Expression, name::display_label};
use reifydb_runtime::{
	context::RuntimeContext,
	hash::{Hash128, xxh3_128},
};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};

use super::{accumulator::SlotKind, aggregate::build_aggregation_shape};
use crate::operator::OperatorCell;

static EMPTY_PARAMS: Params = Params::None;

static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

#[derive(Clone, Debug)]
pub enum SlotInput {
	Star,
	Column(String),
	Expr(usize),
}

enum SlotArg {
	Star,
	Column(String),
	Expr(Expression),
}

fn classify_slot(routines: &Routines, expr: &Expression) -> Option<(SlotKind, SlotArg)> {
	let inner = match expr {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	let call = match inner {
		Expression::Call(c) => c,
		_ => return None,
	};
	let name = call.func.0.text().to_string();
	routines.get_aggregate_function(&name)?;
	let arg = match call.args.as_slice() {
		[] => SlotArg::Star,
		[Expression::Column(col)] => SlotArg::Column(col.0.name.text().to_string()),
		[single] => SlotArg::Expr(single.clone()),
		_ => return None,
	};
	let is_star = matches!(arg, SlotArg::Star);
	let short = name.rsplit("::").next().unwrap_or(&name);
	let kind = match short {
		"count" => SlotKind::Count {
			count_star: is_star,
		},
		"sum" if !is_star => SlotKind::Sum,
		"avg" if !is_star => SlotKind::Avg,
		"min" if !is_star => SlotKind::Min,
		"max" if !is_star => SlotKind::Max,
		_ => return None,
	};
	Some((kind, arg))
}

fn synthetic_aggregate_column(idx: usize) -> Expression {
	let name = format!("__aggregate{idx}");
	Expression::Column(ColumnExpression(ColumnIdentifier {
		shape: ColumnShape::Alias(Fragment::internal(name.clone())),
		name: Fragment::internal(name),
	}))
}

fn rewrite_aggregates(routines: &Routines, expr: &mut Expression, slots: &mut Vec<(SlotKind, SlotArg)>) -> bool {
	if let Some((kind, arg)) = classify_slot(routines, expr) {
		let idx = slots.len();
		slots.push((kind, arg));
		*expr = synthetic_aggregate_column(idx);
		return true;
	}
	match expr {
		Expression::Alias(a) => rewrite_aggregates(routines, a.expression.as_mut(), slots),
		Expression::Cast(c) => rewrite_aggregates(routines, c.expression.as_mut(), slots),
		Expression::Prefix(p) => rewrite_aggregates(routines, p.expression.as_mut(), slots),
		Expression::Add(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Sub(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Mul(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Div(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Rem(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Constant(_) => true,
		_ => false,
	}
}

pub struct Aggregation {
	pub node: FlowNodeId,
	pub parent: OperatorCell,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub compiled_group_by: Vec<CompiledExpr>,
	pub group_names: Vec<String>,
	pub aggregate_output_names: Vec<String>,

	pub slot_kinds: Option<Vec<SlotKind>>,

	pub slot_inputs: Vec<SlotInput>,

	pub compiled_slot_args: Vec<CompiledExpr>,

	pub compiled_outputs: Vec<CompiledExpr>,

	pub routines: Routines,
	pub runtime_context: RuntimeContext,
}

impl Aggregation {
	pub fn new(
		node: FlowNodeId,
		parent: OperatorCell,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
	) -> Self {
		let symbols = SymbolTable::new();
		let compile_ctx = CompileContext {
			symbols: &symbols,
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
			if rewrite_aggregates(&routines, &mut expr, &mut slots) {
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
			node,
			parent,
			group_by,
			aggregations,
			compiled_group_by,
			group_names,
			aggregate_output_names,
			slot_kinds,
			slot_inputs,
			compiled_slot_args,
			compiled_outputs,
			routines,
			runtime_context,
		}
	}

	pub fn create_window_key(&self, group_hash: Hash128, window_id: u64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"win:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(window_id);
		serializer.finish()
	}

	pub(super) fn create_engine_meta_key(&self, group_hash: Hash128, window_start: u64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"ewm:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(window_start);
		serializer.finish()
	}

	pub fn compute_groups(&self, columns: &Columns) -> Result<Vec<(Hash128, Vec<Value>)>> {
		let row_count = columns.row_count();
		if row_count == 0 {
			return Ok(Vec::new());
		}
		if self.compiled_group_by.is_empty() {
			return Ok(vec![(Hash128::from(0u128), Vec::new()); row_count]);
		}

		let session = self.eval_session(false);
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
					Error(Box::new(internal!("Failed to encode group-by value: {}", e)))
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
		let session = self.eval_session(false);
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
		let names: Vec<String> = (0..slot_values.len()).map(|i| format!("__aggregate{i}")).collect();
		let types: Vec<_> = slot_values.iter().map(Value::get_type).collect();
		let layout = build_aggregation_shape(&names, &types);
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, slot_values);
		let row = Row {
			number: RowNumber(0),
			encoded,
			shape: layout,
		};
		let columns = Columns::from_row(&row);
		let session = self.eval_session(false);
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
		ts_nanos: u64,
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
		encoded.set_timestamps(ts_nanos, ts_nanos);
		Ok(Row {
			number: row_number,
			encoded,
			shape: layout,
		})
	}

	pub fn current_timestamp(&self) -> u64 {
		self.runtime_context.clock.now_millis()
	}

	pub(super) fn eval_session(&self, is_aggregate: bool) -> EvalContext<'_> {
		EvalContext {
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: is_aggregate,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		}
	}
}
