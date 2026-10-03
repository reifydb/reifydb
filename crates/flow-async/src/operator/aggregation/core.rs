// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::repeat_n, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use postcard::to_stdvec;
use reifydb_core::{
	error::diagnostic::{
		flow::{
			flow_digest_accuracy_given_for_digest, flow_digest_accuracy_required,
			flow_digest_input_rejected,
		},
		operation::aggregate_group_by_unkeyable,
	},
	expression::{Expression, name::display_label},
	interface::{catalog::flow::OperatorId, change::Diff},
	internal_err,
	value::{
		batch::{batch, batch_with, empty_batch},
		column::builder::ColumnBuilder,
	},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::{CompileContext, EvalContext},
};
use reifydb_flow::{
	aggregate::{
		AggregateContext, DigestSlots, SlotArg, SlotKind, rewrite_aggregates, synthetic_aggregate_column_name,
	},
	compiler::operator::aggregate_validation::aggregate_call_diagnostic,
	context::FlowContext,
	operator::map::schema_column,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	reifydb_assertions,
	util::hash::{Hash128, xxh3_128},
	value::{
		Value,
		column_view::ColumnView,
		constraint::{precision::Precision, scale::Scale},
		container::temporal_array::datetime_array,
		datetime::DateTime,
		digest::{Digest, DigestError},
		row_number::RowNumber,
		system_columns::{SystemColumn, column_view, system_field},
		value_type::{
			ValueType,
			field::{FieldType, from_field, named, to_field},
		},
	},
};

use crate::{
	error::FlowStateError,
	operator::aggregation::accumulator::RowAccumulator,
	window::{engine::tumbling::TumblingEngine, span::WindowSpan},
};

#[derive(Clone, Debug)]
pub enum SlotInput {
	Star,
	Column(String),
	Expr(usize),
	EventTime,
}

fn check_digest_input(function: &str, accuracy: Option<u32>, data: &ColumnView<'_>) -> Result<()> {
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

fn bare_slot(output: &Expression, slot_count: usize) -> Option<usize> {
	match output {
		Expression::Alias(alias) => bare_slot(&alias.expression, slot_count),
		Expression::Column(column) => {
			let name = column.0.name.text();
			(0..slot_count).find(|slot| synthetic_aggregate_column_name(*slot) == name)
		}
		_ => None,
	}
}

enum SlotView<'a> {
	Absent,
	Column(ColumnView<'a>),
	EventTime,
}

pub struct SlotViews<'a>(Vec<SlotView<'a>>);

impl SlotViews<'_> {
	pub fn contribution(&self, row_idx: usize, event_time: DateTime) -> Vec<Option<Value>> {
		self.0.iter()
			.map(|view| match view {
				SlotView::Absent => None,
				SlotView::Column(column) => Some(column.get_value(row_idx)),
				SlotView::EventTime => Some(Value::DateTime(event_time)),
			})
			.collect()
	}
}

fn declared_slot_type(kind: SlotKind, input: &SlotInput, parent_schema: Option<&SchemaRef>) -> Option<ValueType> {
	match kind {
		SlotKind::Count {
			..
		} => Some(ValueType::Int8),
		SlotKind::WindowStart | SlotKind::WindowEnd | SlotKind::WindowLast => Some(ValueType::DateTime),
		SlotKind::WindowDuration => Some(ValueType::Duration),
		SlotKind::Min | SlotKind::Max | SlotKind::First | SlotKind::Last => match input {
			SlotInput::Column(name) => {
				let field = parent_schema?.field_with_name(name).ok()?;
				match from_field(field).ok()?.value_type?.inner_type() {
					ValueType::Any => None,
					inner => Some(inner.clone()),
				}
			}
			_ => None,
		},
		SlotKind::Sum
		| SlotKind::Avg
		| SlotKind::Digest {
			..
		} => None,
	}
}

fn type_family(value_type: &ValueType) -> ValueType {
	match value_type {
		ValueType::Decimal {
			..
		} => ValueType::decimal(Precision::MAX, Scale::new(0)),
		other => other.clone(),
	}
}

fn value_family(value: &Value) -> Option<ValueType> {
	match value {
		Value::None {
			..
		} => None,
		other => Some(type_family(&other.get_type())),
	}
}

fn type_groups(signatures: Vec<Vec<Option<ValueType>>>) -> Vec<Vec<usize>> {
	let mut groups: Vec<(Vec<Option<ValueType>>, Vec<usize>)> = Vec::new();
	for (index, signature) in signatures.into_iter().enumerate() {
		let fits = |known: &[Option<ValueType>]| {
			known.iter()
				.zip(&signature)
				.all(|(known, next)| known.is_none() || next.is_none() || known == next)
		};
		match groups.iter_mut().find(|(known, _)| fits(known)) {
			Some((known, members)) => {
				for (known, next) in known.iter_mut().zip(&signature) {
					if known.is_none() {
						*known = next.clone();
					}
				}
				members.push(index);
			}
			None => groups.push((signature, vec![index])),
		}
	}
	groups.into_iter().map(|(_, members)| members).collect()
}

fn typed_column<'v>(
	name: &str,
	declared: Option<ValueType>,
	values: impl Iterator<Item = &'v Value>,
) -> Result<(FieldRef, ArrayRef)> {
	let values: Vec<&Value> = values.collect();
	let column_type = match declared {
		Some(declared) => {
			let family = type_family(declared.inner_type());
			if let Some(value) =
				values.iter().find(|value| value_family(value).is_some_and(|got| got != family))
			{
				return internal_err!(
					"aggregation column {} is declared {:?} but holds a {:?}",
					name,
					declared,
					value.get_type()
				);
			}
			declared
		}
		None => values
			.iter()
			.find(|value| !matches!(value, Value::None { .. }))
			.or(values.first())
			.map_or(ValueType::Any, |value| value.get_type()),
	};
	let mut builder = ColumnBuilder::with_capacity(column_type, values.len());
	for value in values {
		builder.push_value(value.clone());
	}
	Ok(builder.finish(name))
}

fn optional_named(name: &str, field: &FieldRef, array: ArrayRef) -> Result<(FieldRef, ArrayRef)> {
	let mut field_type = from_field(field)?;
	field_type.value_type = field_type.value_type.map(|value_type| match value_type {
		ValueType::Option(_) => value_type,
		other => ValueType::Option(Box::new(other)),
	});
	Ok(named(name, field_type, array))
}

#[derive(Clone, Debug)]
pub struct EmitRow {
	pub group_values: Vec<Value>,
	pub slot_values: Vec<Value>,
	pub row_number: RowNumber,
	pub span: Option<WindowSpan<DateTime>>,
}

fn row_types(row: &EmitRow) -> Vec<Option<ValueType>> {
	row.group_values.iter().chain(&row.slot_values).map(value_family).collect()
}

pub struct Aggregation {
	pub operator: OperatorId,
	pub output_schema: SchemaRef,
	pub compiled_group_by: Vec<CompiledExpr>,
	pub group_names: Vec<String>,
	pub aggregate_output_names: Vec<String>,

	pub slot_kinds: Option<Vec<SlotKind>>,

	pub slot_inputs: Vec<SlotInput>,

	pub compiled_slot_args: Vec<CompiledExpr>,

	pub compiled_outputs: Vec<CompiledExpr>,

	bare_outputs: Option<Vec<usize>>,

	slot_types: Vec<Option<ValueType>>,

	emit_schema: Option<SchemaRef>,

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
		parent_schema: Option<SchemaRef>,
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
		let slot_count = slots.len();
		let (slot_kinds, slot_inputs, compiled_slot_args, compiled_outputs, bare_outputs) = if all_representable
		{
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
			let bare: Option<Vec<usize>> =
				rewritten_outputs.iter().map(|output| bare_slot(output, slot_count)).collect();
			(Some(kinds), inputs, compiled_args, outputs, bare)
		} else {
			(None, Vec::new(), Vec::new(), Vec::new(), None)
		};
		let group_names: Vec<String> = group_by.iter().map(|e| display_label(e).text().to_string()).collect();
		let slot_types: Vec<Option<ValueType>> = match &slot_kinds {
			Some(kinds) => kinds
				.iter()
				.zip(&slot_inputs)
				.map(|(kind, input)| declared_slot_type(*kind, input, parent_schema.as_ref()))
				.collect(),
			None => Vec::new(),
		};
		let aggregate_fields = aggregations.iter().enumerate().map(|(index, expression)| {
			let declared =
				bare_outputs.as_ref().and_then(|bare| slot_types.get(bare[index]).cloned().flatten());
			match declared {
				Some(declared) => Arc::new(to_field(
					&aggregate_output_names[index],
					&FieldType::from(ValueType::Option(Box::new(declared))),
				)),
				None => schema_column(parent_schema.as_ref(), expression),
			}
		});
		let output_schema = Arc::new(Schema::new(
			group_by.iter()
				.map(|e| schema_column(parent_schema.as_ref(), e))
				.chain(aggregate_fields)
				.collect::<Vec<FieldRef>>(),
		));

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
			bare_outputs,
			slot_types,
			emit_schema: None,
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

	pub fn compute_groups(&self, columns: &RecordBatch) -> Result<Vec<(Hash128, Vec<Value>)>> {
		let row_count = columns.num_rows();
		if row_count == 0 {
			return Ok(Vec::new());
		}
		if self.compiled_group_by.is_empty() {
			return Ok(vec![(Hash128::from(0u128), Vec::new()); row_count]);
		}

		let session = self.eval_session();
		let exec_ctx = session.with_eval(columns.clone(), row_count);
		let mut group_columns: Vec<(FieldRef, ArrayRef)> = Vec::new();
		for compiled_expr in &self.compiled_group_by {
			let column = compiled_expr.execute(&exec_ctx)?;
			let ty = ColumnView::try_from(&column)?.get_type();
			if !ty.is_scalar() {
				return Err(Error(Box::new(aggregate_group_by_unkeyable(
					Fragment::internal(column.0.name()),
					ty,
				))));
			}
			group_columns.push(column);
		}
		let group_views = group_columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;

		let mut out = Vec::with_capacity(row_count);
		let mut buf = Vec::with_capacity(128);
		for row_idx in 0..row_count {
			buf.clear();
			let mut values = Vec::with_capacity(group_views.len());
			for col in &group_views {
				let value = col.get_value(row_idx);
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

	pub fn evaluate_slot_inputs(&self, columns: &RecordBatch) -> Result<Vec<(FieldRef, ArrayRef)>> {
		let mut out = Vec::with_capacity(self.compiled_slot_args.len());
		if !self.compiled_slot_args.is_empty() {
			let row_count = columns.num_rows();
			let session = self.eval_session();
			let exec_ctx = session.with_eval(columns.clone(), row_count);
			for compiled in &self.compiled_slot_args {
				out.push(compiled.execute(&exec_ctx)?);
			}
		}
		self.check_digest_inputs(columns, &out)?;
		Ok(out)
	}

	fn check_digest_inputs(&self, columns: &RecordBatch, slot_cols: &[(FieldRef, ArrayRef)]) -> Result<()> {
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
				SlotInput::Column(name) => match column_view(columns, name)? {
					Some(column) => column,
					None => continue,
				},
				SlotInput::Expr(idx) => ColumnView::try_from(&slot_cols[*idx])?,
				SlotInput::Star | SlotInput::EventTime => continue,
			};
			check_digest_input(self.digests.function_written(slot), *accuracy, &data)?;
		}
		Ok(())
	}

	pub fn slot_views<'a>(
		&self,
		columns: &'a RecordBatch,
		slot_cols: &'a [(FieldRef, ArrayRef)],
	) -> Result<SlotViews<'a>> {
		if columns.num_rows() == 0 {
			return Ok(SlotViews(Vec::new()));
		}
		let views =
			self.slot_inputs
				.iter()
				.map(|input| -> Result<SlotView<'a>> {
					Ok(match input {
						SlotInput::Star => SlotView::Absent,
						SlotInput::Column(name) => column_view(columns, name)?
							.map_or(SlotView::Absent, SlotView::Column),
						SlotInput::Expr(idx) => {
							SlotView::Column(ColumnView::try_from(&slot_cols[*idx])?)
						}
						SlotInput::EventTime => SlotView::EventTime,
					})
				})
				.collect::<Result<Vec<_>>>()?;
		Ok(SlotViews(views))
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

	pub fn emit_diffs(
		&mut self,
		inserts: Vec<EmitRow>,
		updates: Vec<(EmitRow, EmitRow)>,
		removes: Vec<EmitRow>,
		ts: DateTime,
	) -> Result<Vec<Diff>> {
		let inserts: Vec<EmitRow> = inserts.into_iter().map(|row| self.patched(row)).collect();
		let updates: Vec<(EmitRow, EmitRow)> =
			updates.into_iter().map(|(pre, post)| (self.patched(pre), self.patched(post))).collect();
		let removes: Vec<EmitRow> = removes.into_iter().map(|row| self.patched(row)).collect();
		let mut diffs = Vec::new();
		for members in type_groups(inserts.iter().map(row_types).collect()) {
			let rows: Vec<&EmitRow> = members.iter().map(|&index| &inserts[index]).collect();
			diffs.push(Diff::insert(self.emit_batch(&rows, ts)?));
		}
		let signatures = updates
			.iter()
			.map(|(pre, post)| row_types(pre).into_iter().chain(row_types(post)).collect())
			.collect();
		for members in type_groups(signatures) {
			let pres: Vec<&EmitRow> = members.iter().map(|&index| &updates[index].0).collect();
			let posts: Vec<&EmitRow> = members.iter().map(|&index| &updates[index].1).collect();
			diffs.push(Diff::update(self.emit_batch(&pres, ts)?, self.emit_batch(&posts, ts)?));
		}
		for members in type_groups(removes.iter().map(row_types).collect()) {
			let rows: Vec<&EmitRow> = members.iter().map(|&index| &removes[index]).collect();
			diffs.push(Diff::remove(self.emit_batch(&rows, ts)?));
		}
		Ok(diffs)
	}

	fn patched(&self, mut row: EmitRow) -> EmitRow {
		if let Some(span) = row.span
			&& let Some(patched) = self.span_slot_values(&row.slot_values, span)
		{
			row.slot_values = patched;
		}
		row
	}

	fn emit_batch(&mut self, rows: &[&EmitRow], ts: DateTime) -> Result<RecordBatch> {
		let count = rows.len();
		let slot_count = rows.first().map_or(0, |row| row.slot_values.len());
		let slot_columns = (0..slot_count)
			.map(|slot| {
				typed_column(
					&synthetic_aggregate_column_name(slot),
					self.slot_types.get(slot).cloned().flatten(),
					rows.iter().map(|row| &row.slot_values[slot]),
				)
			})
			.collect::<Result<Vec<_>>>()?;
		let outputs = self.output_columns(slot_columns, count)?;
		let mut columns: Vec<(FieldRef, ArrayRef)> =
			Vec::with_capacity(self.group_names.len() + outputs.len() + 4);
		for (index, name) in self.group_names.iter().enumerate() {
			let declared = from_field(self.output_schema.field(index))?
				.value_type
				.filter(|value_type| !matches!(value_type.inner_type(), ValueType::Any));
			columns.push(typed_column(name, declared, rows.iter().map(|row| &row.group_values[index]))?);
		}
		for ((field, array), name) in outputs.into_iter().zip(&self.aggregate_output_names) {
			columns.push(optional_named(name, &field, array)?);
		}
		let system = |column: SystemColumn, array: ArrayRef| {
			(system_field(column, array.logical_null_count() > 0), array)
		};
		let stamps: ArrayRef = Arc::new(datetime_array(repeat_n(ts, count)));
		columns.push(system(
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.row_number.0))),
		));
		columns.push(system(SystemColumn::CreatedAt, stamps.clone()));
		columns.push(system(SystemColumn::UpdatedAt, stamps));
		columns.push(system(
			SystemColumn::Time,
			Arc::new(datetime_array(rows.iter().map(|row| row.span.map_or(ts, |span| span.start)))),
		));
		let out = match &self.emit_schema {
			Some(schema) => batch_with(schema, columns, count)?,
			None => batch(columns)?,
		};
		self.emit_schema = Some(out.schema());
		Ok(out)
	}

	fn output_columns(
		&self,
		slot_columns: Vec<(FieldRef, ArrayRef)>,
		count: usize,
	) -> Result<Vec<(FieldRef, ArrayRef)>> {
		if self.compiled_outputs.is_empty() {
			return Ok(slot_columns);
		}
		let Some(bare) = &self.bare_outputs else {
			return self.evaluated_columns(&slot_columns, count);
		};
		let out: Vec<(FieldRef, ArrayRef)> = bare.iter().map(|&slot| slot_columns[slot].clone()).collect();
		reifydb_assertions! {
			let evaluated = self.evaluated_columns(&slot_columns, count)?;
			for (index, ((bare_field, bare_array), (field, array))) in out.iter().zip(&evaluated).enumerate() {
				let bare_view = ColumnView::try_from((bare_array, bare_field.as_ref()))?;
				let view = ColumnView::try_from((array, field.as_ref()))?;
				assert!(
					bare_view.get_type() == view.get_type()
						&& (0..count).all(|row| bare_view.get_value(row) == view.get_value(row)),
					"a bare aggregate output must publish exactly what evaluating it on the slot batch returns; \
					 output {index} differs"
				);
			}
		}
		Ok(out)
	}

	fn evaluated_columns(
		&self,
		slot_columns: &[(FieldRef, ArrayRef)],
		count: usize,
	) -> Result<Vec<(FieldRef, ArrayRef)>> {
		let session = self.eval_session();
		let exec_ctx = session.with_eval(batch(slot_columns.to_vec())?, count);
		self.compiled_outputs.iter().map(|compiled| compiled.execute(&exec_ctx)).collect()
	}

	fn eval_session(&self) -> EvalContext<'_> {
		EvalContext {
			params: &self.ctx.params,
			symbols: &self.ctx.symbols,
			routines: &self.routines,
			runtime_context: &self.runtime_context,
			identity: self.ctx.identity,
			is_aggregate_context: false,
			batch: empty_batch(),
			row_count: 1,
			target: None,
			take: None,
		}
	}
}

#[cfg(test)]
mod tests {
	use arrow_array::ArrayRef;
	use arrow_schema::FieldRef;
	use reifydb_core::value::column::builder::ColumnBuilder;
	use reifydb_flow::aggregate::DIGEST_FUNCTION;
	use reifydb_value::value::{
		Value, column_view::ColumnView, digest::Digest, duration::Duration, value_type::ValueType,
	};

	use super::{check_digest_input, typed_column};

	const PPM: u32 = 10_000;

	fn column(values: Vec<Value>) -> (FieldRef, ArrayRef) {
		let first_type = values.iter().find(|value| !matches!(value, Value::None { .. })).map(Value::get_type);
		let mut builder = ColumnBuilder::with_capacity(first_type.unwrap_or(ValueType::Float8), values.len());
		for value in values {
			builder.push_value(value);
		}
		builder.finish("v")
	}

	fn digest_of(values: &[f64]) -> Value {
		let mut digest = Digest::new(ValueType::Float8, PPM).unwrap();
		for v in values {
			digest.add_value(&Value::float8(*v)).unwrap();
		}
		Value::Digest(Box::new(digest))
	}

	fn code(accuracy: Option<u32>, values: Vec<Value>) -> String {
		check_digest_input(DIGEST_FUNCTION, accuracy, &ColumnView::try_from(&column(values)).unwrap())
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
			let data = column(values.clone());
			check_digest_input(DIGEST_FUNCTION, accuracy, &ColumnView::try_from(&data).unwrap())
				.unwrap_or_else(|err| {
					panic!("{values:?} with accuracy {accuracy:?} must pass, got {err}")
				});
		}
	}

	#[test]
	fn digest_slot_values_pass_through_the_slot_column_and_read_back_equal() {
		// A slot column that cannot hold a digest panics the flow on the first group that reads a percentile.
		let digest = digest_of(&[1.0, 2.0, 40.0]);
		let digest_type = digest.get_type();
		let values = [digest.clone(), Value::none_of(digest_type.clone()), digest_of(&[])];

		let (field, array) = typed_column("d", None, values.iter()).unwrap();
		let view = ColumnView::try_from((&array, field.as_ref())).unwrap();

		assert_eq!(view.get_type().inner_type(), &digest_type);
		assert_eq!(view.get_value(0), digest);
		assert!(matches!(view.get_value(1), Value::None { .. }));
		assert_eq!(view.get_value(2), values[2]);
	}
}
