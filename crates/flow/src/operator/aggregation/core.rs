// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use postcard::to_stdvec;
use reifydb_codec::row::{
	bytes::RowBuilder,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::{
	error::diagnostic::{
		flow::{
			flow_digest_accuracy_given_for_digest, flow_digest_accuracy_required,
			flow_digest_input_rejected,
		},
		operation::aggregate_group_by_unkeyable,
	},
	interface::catalog::flow::OperatorId,
	row::Row,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::{
	expression::{Expression, name::display_label},
	flow::{
		aggregate::{
			AggregateContext, DigestSlots, SlotArg, SlotKind, rewrite_aggregates,
			synthetic_aggregate_column_name,
		},
		compiler::operator::aggregate_validation::aggregate_call_diagnostic,
	},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::{Hash128, xxh3_128},
	value::{
		Value,
		datetime::DateTime,
		digest::{Digest, DigestError},
		row_number::RowNumber,
		value_type::ValueType,
	},
};

use crate::{
	context::FlowContext,
	error::FlowStateError,
	operator::{aggregation::accumulator::RowAccumulator, map::schema_column},
	window::{engine::tumbling::TumblingEngine, span::WindowSpan},
};

#[derive(Clone, Debug)]
pub enum SlotInput {
	Star,
	Column(String),
	Expr(usize),
	EventTime,
}

fn check_digest_input(function: &str, accuracy: Option<u32>, data: &ColumnBuffer) -> Result<()> {
	let mut probe: Option<Digest> = None;
	for row in 0..data.len() {
		let value = data.get_value(row);
		match (accuracy, &value) {
			(
				_,
				Value::None {
					..
				},
			) => {}
			(Some(_), Value::Digest(_)) => {
				return Err(Error(Box::new(flow_digest_accuracy_given_for_digest(
					function,
					value.get_type(),
				))));
			}
			(Some(accuracy), value) => {
				let digest = match &mut probe {
					Some(digest) => digest,
					None => probe.insert(Digest::new(value.get_type(), accuracy)
						.map_err(|error| digest_input_error(function, error))?),
				};
				digest.add_value(value).map_err(|error| digest_input_error(function, error))?;
			}
			(None, Value::Digest(_)) => {}
			(None, value) => {
				return Err(Error(Box::new(flow_digest_accuracy_required(function, value.get_type()))));
			}
		}
	}
	Ok(())
}

fn digest_input_error(function: &str, error: DigestError) -> Error {
	Error(Box::new(flow_digest_input_rejected(function, error.to_string())))
}

#[inline]
fn build_aggregation_shape(names: &[String], types: &[ValueType]) -> RowShape {
	let fields: Vec<RowShapeField> = names
		.iter()
		.zip(types.iter())
		.map(|(name, ty)| RowShapeField::unconstrained(name.clone(), ty.clone()))
		.collect();
	RowShape::new(RowFamily::Table, fields)
}

pub struct Aggregation {
	pub operator: OperatorId,
	pub output_schema: Columns,
	pub compiled_group_by: Vec<CompiledExpr>,
	pub group_names: Vec<String>,
	pub aggregate_output_names: Vec<String>,

	pub slot_kinds: Option<Vec<SlotKind>>,

	pub slot_inputs: Vec<SlotInput>,

	pub compiled_slot_args: Vec<CompiledExpr>,

	pub compiled_outputs: Vec<CompiledExpr>,

	pub routines: Routines,
	pub runtime_context: RuntimeContext,
	tumbling_engine: Option<Box<TumblingEngine<Hash128, DateTime, RowAccumulator>>>,
	digests: DigestSlots,
	pub ctx: Arc<FlowContext>,
}

impl Aggregation {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		operator: OperatorId,
		parent_schema: Option<Columns>,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
		context: AggregateContext,
		ctx: Arc<FlowContext>,
	) -> Result<Self> {
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};

		let compiled_group_by: Vec<CompiledExpr> =
			group_by.iter().map(|e| compile_expression(&compile_ctx, e)).collect::<Result<Vec<_>>>()?;

		let aggregate_output_names: Vec<String> =
			aggregations.iter().map(|e| display_label(e).text().to_string()).collect();

		let mut slots: Vec<(SlotKind, SlotArg)> = Vec::new();
		let mut digests = DigestSlots::default();
		let mut rewritten_outputs: Vec<Expression> = Vec::new();
		let mut all_representable = !aggregations.is_empty();
		for aggregate in &aggregations {
			let mut expr = aggregate.clone();
			let representable = rewrite_aggregates(&routines, &mut expr, &mut slots, &mut digests, context)
				.map_err(|error| {
					Error(Box::new(aggregate_call_diagnostic(
						display_label(aggregate).text(),
						error,
					)))
				})?;
			if representable {
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
						compiled_args.push(compile_expression(&compile_ctx, &expr)?);
						SlotInput::Expr(idx)
					}
					SlotArg::EventTime => SlotInput::EventTime,
				});
			}
			let outputs: Vec<CompiledExpr> = rewritten_outputs
				.iter()
				.map(|e| compile_expression(&compile_ctx, e))
				.collect::<Result<Vec<_>>>()?;
			(Some(kinds), inputs, compiled_args, outputs)
		} else {
			(None, Vec::new(), Vec::new(), Vec::new())
		};
		let group_names: Vec<String> = group_by.iter().map(|e| display_label(e).text().to_string()).collect();
		let output_schema = Columns::new(
			group_by.iter()
				.chain(&aggregations)
				.map(|e| schema_column(parent_schema.as_ref(), e))
				.collect(),
		);

		Ok(Self {
			operator,
			output_schema,
			compiled_group_by,
			group_names,
			aggregate_output_names,
			slot_kinds,
			slot_inputs,
			compiled_slot_args,
			compiled_outputs,
			routines,
			runtime_context,
			tumbling_engine: None,
			digests,
			ctx,
		})
	}

	pub(crate) fn tumbling_engine_slot(
		&mut self,
	) -> &mut Option<Box<TumblingEngine<Hash128, DateTime, RowAccumulator>>> {
		&mut self.tumbling_engine
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
			let column = compiled_expr.execute(&exec_ctx)?;
			let ty = column.data().get_type();
			if !ty.is_scalar() {
				return Err(Error(Box::new(aggregate_group_by_unkeyable(column.name_owned(), ty))));
			}
			group_columns.push(column);
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
		let mut out = Vec::with_capacity(self.compiled_slot_args.len());
		if !self.compiled_slot_args.is_empty() {
			let row_count = columns.row_count();
			let session = self.eval_session();
			let exec_ctx = session.with_eval(columns.clone(), row_count);
			for compiled in &self.compiled_slot_args {
				out.push(compiled.execute(&exec_ctx)?);
			}
		}
		self.check_digest_inputs(columns, &out)?;
		Ok(out)
	}

	fn check_digest_inputs(&self, columns: &Columns, slot_cols: &[ColumnWithName]) -> Result<()> {
		let Some(kinds) = &self.slot_kinds else {
			return Ok(());
		};
		for (slot, (kind, input)) in kinds.iter().zip(self.slot_inputs.iter()).enumerate() {
			let SlotKind::Digest {
				accuracy,
			} = kind
			else {
				continue;
			};
			let data = match input {
				SlotInput::Column(name) => match columns.column(name) {
					Some(column) => column.data(),
					None => continue,
				},
				SlotInput::Expr(idx) => slot_cols[*idx].data(),
				SlotInput::Star | SlotInput::EventTime => continue,
			};
			check_digest_input(self.digests.function_written(slot), *accuracy, data)?;
		}
		Ok(())
	}

	pub fn build_contribution(
		&self,
		columns: &Columns,
		slot_cols: &[ColumnWithName],
		row_idx: usize,
		event_time: DateTime,
	) -> Vec<Option<Value>> {
		self.slot_inputs
			.iter()
			.map(|input| match input {
				SlotInput::Star => None,
				SlotInput::Column(name) => columns.column(name).map(|c| c.data().get_value(row_idx)),
				SlotInput::Expr(idx) => Some(slot_cols[*idx].data().get_value(row_idx)),
				SlotInput::EventTime => Some(Value::DateTime(event_time)),
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
		let mut encoded = layout.allocate_table();
		layout.set_values(&mut encoded, slot_values);
		let row = Row {
			number: RowNumber(0),
			encoded: encoded.freeze_bytes(),
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

	pub fn needs_event_time(&self) -> bool {
		self.slot_inputs.iter().any(|input| matches!(input, SlotInput::EventTime))
	}

	fn span_slot_values(&self, slot_values: &[Value], span: WindowSpan<DateTime>) -> Option<Vec<Value>> {
		let kinds = self.slot_kinds.as_ref()?;
		if !kinds.iter().any(|kind| kind.requires_span()) {
			return None;
		}
		let mut out = slot_values.to_vec();
		for (value, kind) in out.iter_mut().zip(kinds.iter()) {
			*value = match kind {
				SlotKind::WindowStart => Value::DateTime(span.start),
				SlotKind::WindowEnd => Value::DateTime(span.end),
				SlotKind::WindowDuration => {
					Value::Duration(span.end.saturating_duration_since(span.start))
				}
				_ => continue,
			};
		}
		Some(out)
	}

	pub fn build_engine_row(
		&self,
		group_values: &[Value],
		slot_values: &[Value],
		row_number: RowNumber,
		ts: DateTime,
		span: Option<WindowSpan<DateTime>>,
	) -> Result<Row> {
		let patched = span.and_then(|span| self.span_slot_values(slot_values, span));
		let aggregate_values = self.compute_outputs(patched.as_deref().unwrap_or(slot_values))?;
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
		let mut encoded = layout.allocate_table();
		layout.set_values(&mut encoded, &values);
		encoded.set_timestamps(ts, ts);
		encoded.set_time(span.map(|span| span.start).unwrap_or(ts));
		Ok(Row {
			number: row_number,
			encoded: encoded.freeze_bytes(),
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

#[cfg(test)]
mod tests {
	use reifydb_codec::row::bytes::RowBuilder;
	use reifydb_core::{
		row::Row,
		value::column::{buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_rql::flow::aggregate::DIGEST_FUNCTION;
	use reifydb_value::value::{
		Value, digest::Digest, duration::Duration, row_number::RowNumber, value_type::ValueType,
	};

	use super::{build_aggregation_shape, check_digest_input};

	const PPM: u32 = 10_000;

	fn column(values: Vec<Value>) -> ColumnBuffer {
		let mut data = ColumnBuffer::none_typed(ValueType::Float8, 0);
		for value in values {
			data.push_value(value);
		}
		data
	}

	fn digest_of(values: &[f64]) -> Value {
		let mut digest = Digest::new(ValueType::Float8, PPM).unwrap();
		for v in values {
			digest.add_value(&Value::float8(*v)).unwrap();
		}
		Value::Digest(Box::new(digest))
	}

	fn code(accuracy: Option<u32>, values: Vec<Value>) -> String {
		check_digest_input(DIGEST_FUNCTION, accuracy, &column(values))
			.expect_err("the input must be refused")
			.0
			.code
	}

	#[test]
	fn raw_values_without_an_accuracy_are_refused_before_they_reach_the_slot() {
		// A slot without an accuracy cannot build a digest, so letting raw values through panics the flow.
		assert_eq!(code(None, vec![Value::none(), Value::float8(1.5)]), "FLOW_055");
	}

	#[test]
	fn a_digest_input_with_an_accuracy_is_refused_before_it_reaches_the_slot() {
		// Adding a stored digest as one raw value would count it once instead of merging its rows.
		assert_eq!(code(Some(PPM), vec![digest_of(&[1.0, 2.0])]), "FLOW_056");
	}

	#[test]
	fn a_value_the_digest_cannot_hold_is_refused_even_after_valid_rows() {
		// A month-part duration has no fixed length, so a digest that took it would place it in the wrong
		// bucket.
		let values = vec![
			Value::Duration(Duration::from_days(2).unwrap()),
			Value::Duration(Duration::from_months(1).unwrap()),
		];
		assert_eq!(code(Some(PPM), values), "FLOW_057");
	}

	#[test]
	fn accepted_inputs_and_all_none_columns_pass_the_check() {
		// Refusing a valid batch would stall a flow that the slot could have served.
		for (accuracy, values) in [
			(Some(PPM), vec![Value::float8(1.5), Value::none(), Value::float8(-3.0)]),
			(None, vec![digest_of(&[1.0]), Value::none(), digest_of(&[4.0, 9.0])]),
			(None, vec![Value::none(), Value::none()]),
			(Some(PPM), vec![Value::none()]),
		] {
			check_digest_input(DIGEST_FUNCTION, accuracy, &column(values.clone())).unwrap_or_else(|err| {
				panic!("{values:?} with accuracy {accuracy:?} must pass, got {err}")
			});
		}
	}

	#[test]
	fn digest_slot_values_pass_through_the_aggregation_shape_and_read_back_equal() {
		// A slot shape that cannot hold a digest panics the flow on the first group that reads a percentile.
		let digest = digest_of(&[1.0, 2.0, 40.0]);
		let digest_type = digest.get_type();
		let values = vec![Value::Int4(7), digest.clone(), Value::none_of(digest_type.clone()), digest_of(&[])];
		let names: Vec<String> = ["g", "d", "empty_group", "no_values"].map(String::from).to_vec();
		let types: Vec<ValueType> = values.iter().map(Value::get_type).collect();

		let shape = build_aggregation_shape(&names, &types);
		assert_eq!(shape.fingerprint(), build_aggregation_shape(&names, &types).fingerprint());
		let mut encoded = shape.allocate_table();
		shape.set_values(&mut encoded, &values);
		let row = Row {
			number: RowNumber(1),
			encoded: encoded.freeze_bytes(),
			shape,
		};
		let columns = Columns::from_row(&row);

		assert_eq!(columns[0].get_value(0), Value::Int4(7));
		assert_eq!(columns[1].get_type(), digest_type);
		assert_eq!(columns[1].get_value(0), digest);
		assert!(matches!(columns[2].get_value(0), Value::None { .. }));
		assert_eq!(columns[3].get_value(0), values[3]);
	}
}
