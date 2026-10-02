// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{
	error::{
		CoreError,
		diagnostic::{operation, query},
	},
	expression::{CallExpression, ConstantExpression, Expression, name::display_label},
	metrics::heap::HeapSize,
	value::{
		batch::{batch, group_by, take_rows},
		column::{
			factory::rename,
			headers::ColumnHeaders,
			view::group_by::{GroupId, GroupKeyDict, GroupRows},
		},
	},
};
use reifydb_evaluate::expression::{
	compile::{CompiledExpr, compile_expression},
	context::CompileContext,
};
use reifydb_flow::aggregate::{
	DigestSlots, PERCENTILE_FUNCTION, PercentileCallError, rewrite_aggregate_calls, synthetic_aggregate_column_name,
};
use reifydb_routine_abi::{
	Accumulator, LiteralArgument, LiteralKind, context::FunctionContext, error::RoutineError, registry::Routines,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	error::{Error, FunctionErrorKind, TypeError},
	fragment::Fragment,
	reifydb_assertions,
	value::{digest::DigestError, system_columns::column_view, value_type::ValueType},
};
use tracing::instrument;

use super::NoopNode;
use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode, charge_query_memory_bytes, eval_context_from_query},
		udf::UdfEvalNode,
	},
};

enum SlotInput {
	Column {
		name: String,
		fragment: Fragment,
	},
	Expression(usize),
}

struct AggregateSlot {
	input: SlotInput,
	accumulator: Box<dyn Accumulator>,
}

impl AggregateSlot {
	fn update(&mut self, columns: &RecordBatch, inputs: &[(FieldRef, ArrayRef)], groups: &GroupRows) -> Result<()> {
		let argument = match &self.input {
			SlotInput::Column {
				name,
				fragment,
			} => {
				let index = columns
					.schema_ref()
					.fields()
					.iter()
					.position(|field| field.name() == name)
					.ok_or_else(|| error!(query::column_not_found(fragment.clone())))?;
				(columns.schema_ref().fields()[index].clone(), columns.column(index).clone())
			}
			SlotInput::Expression(index) => inputs[*index].clone(),
		};
		self.accumulator.update(&[argument], groups)?;
		Ok(())
	}

	fn finalize(mut self, dict: &GroupKeyDict) -> Result<(FieldRef, ArrayRef)> {
		let (keys_out, data) = self.accumulator.finalize()?;
		align_column_data(dict, &keys_out, data)
	}
}

enum Projection {
	Group {
		column: String,
		alias: Fragment,
	},
	Computed {
		alias: Fragment,
		expression: Expression,
	},
}

struct SlotCall {
	call: CallExpression,
	name: Fragment,
}

struct Aggregation {
	keys: Vec<String>,
	projections: Vec<Projection>,
	slots: Vec<AggregateSlot>,
	inputs: Vec<CompiledExpr>,
}

pub(crate) struct AggregateNode {
	input: Box<dyn QueryNode>,
	by: Vec<Expression>,
	map: Vec<Expression>,
	aggregation: Option<Aggregation>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<QueryContext>>,
}

impl AggregateNode {
	pub fn new(
		input: Box<dyn QueryNode>,
		by: Vec<Expression>,
		map: Vec<Expression>,
		context: Arc<QueryContext>,
	) -> Self {
		Self {
			input,
			by,
			map,
			aggregation: None,
			headers: None,
			context: Some(context),
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::accumulate")]
	fn accumulate<'a>(
		input: &mut Box<dyn QueryNode>,
		rx: &mut Transaction<'a>,
		ctx: &mut QueryContext,
		aggregation: &mut Aggregation,
		dict: &mut GroupKeyDict,
		key_types: &mut Vec<Option<ValueType>>,
	) -> Result<()> {
		let keys: Vec<&str> = aggregation.keys.iter().map(String::as_str).collect();
		let mut charged = 0usize;
		while let Some(columns) = input.next(rx, ctx)? {
			if key_types.is_empty() {
				for key in &keys {
					key_types.push(column_view(&columns, key)?.map(|view| view.get_type()));
				}
				let aliases =
					aggregation.projections.iter().filter_map(|projection| match projection {
						Projection::Group {
							alias,
							..
						} => Some(alias),
						Projection::Computed {
							..
						} => None,
					});
				for (key_type, alias) in key_types.iter().zip(aliases) {
					if let Some(ty) = key_type
						&& !ty.is_scalar()
					{
						return Err(error!(operation::aggregate_group_by_unkeyable(
							alias.clone(),
							ty.clone()
						)));
					}
				}
			}
			let groups = group_by(&columns, &keys, dict)?;

			let row_count = columns.num_rows();
			let evaluation = eval_context_from_query(ctx).with_eval(columns, row_count);
			let inputs = aggregation
				.inputs
				.iter()
				.map(|compiled| compiled.execute(&evaluation))
				.collect::<Result<Vec<_>>>()?;
			let columns = evaluation.batch;

			for slot in aggregation.slots.iter_mut() {
				slot.update(&columns, &inputs, &groups)?;
			}

			let state = dict.heap_size()
				+ aggregation.slots.iter().map(|slot| slot.accumulator.heap_size()).sum::<usize>();
			charge_query_memory_bytes(&ctx.memory, &mut charged, state)?;
		}

		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::finalize")]
	fn finalize(
		projections: Vec<Projection>,
		slots: Vec<AggregateSlot>,
		keys: &[&str],
		dict: &GroupKeyDict,
		key_types: &[Option<ValueType>],
		ctx: &QueryContext,
	) -> Result<Vec<(FieldRef, ArrayRef)>> {
		let slot_columns = slots
			.into_iter()
			.enumerate()
			.map(|(idx, slot)| Ok(rename(slot.finalize(dict)?, &synthetic_aggregate_column_name(idx))))
			.collect::<Result<Vec<_>>>()?;
		let evaluation = eval_context_from_query(ctx).with_eval(batch(slot_columns)?, dict.len());
		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		let mut result_columns = Vec::new();

		for projection in projections {
			match projection {
				Projection::Group {
					alias,
					column,
					..
				} => {
					let col_idx = keys.iter().position(|k| k == &column).unwrap();
					result_columns.push(dict.key_column(
						col_idx,
						key_types.get(col_idx).cloned().flatten(),
						alias.fragment(),
					)?);
				}
				Projection::Computed {
					alias,
					expression,
				} => {
					let compiled = compile_expression(&compile_ctx, &expression)?;
					result_columns.push(rename(compiled.execute(&evaluation)?, alias.fragment()));
				}
			}
		}

		Ok(result_columns)
	}
}

impl QueryNode for AggregateNode {
	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let (keys, projections, slots, input_expressions) =
			parse_keys_and_aggregates(&self.by, &self.map, &ctx.services.routines, ctx)?;
		let (input, input_expressions, _) = UdfEvalNode::wrap_if_needed(
			mem::replace(&mut self.input, Box::new(NoopNode)),
			&input_expressions,
			&ctx.symbols,
		);
		self.input = input;

		let compile_ctx = CompileContext {
			symbols: &ctx.symbols,
		};
		let inputs = input_expressions
			.iter()
			.map(|expression| compile_expression(&compile_ctx, expression))
			.collect::<Result<Vec<_>>>()?;
		self.aggregation = Some(Aggregation {
			keys,
			projections,
			slots,
			inputs,
		});

		self.input.initialize(rx, ctx)?;

		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "AggregateNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let mut aggregation =
			self.aggregation.take().expect("AggregateNode::next() called before initialize()");

		let mut dict = GroupKeyDict::new();
		let mut key_types = Vec::new();

		Self::accumulate(&mut self.input, rx, ctx, &mut aggregation, &mut dict, &mut key_types)?;

		let keys: Vec<&str> = aggregation.keys.iter().map(String::as_str).collect();
		let result_columns = Self::finalize(
			aggregation.projections,
			aggregation.slots,
			&keys,
			&dict,
			&key_types,
			stored_ctx,
		)?;

		let columns = batch(result_columns)?;
		self.headers = Some(ColumnHeaders::from_batch(&columns));

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}

type ParsedKeysAndAggregates = (Vec<String>, Vec<Projection>, Vec<AggregateSlot>, Vec<Expression>);

fn parse_keys_and_aggregates(
	by: &[Expression],
	project: &[Expression],
	routines: &Routines,
	ctx: &QueryContext,
) -> Result<ParsedKeysAndAggregates> {
	let mut keys = Vec::new();
	let mut projections = Vec::new();
	let mut inputs = Vec::new();

	for gb in by {
		match gb {
			Expression::Column(c) => {
				keys.push(c.0.name.text().to_string());
				projections.push(Projection::Group {
					column: c.0.name.text().to_string(),
					alias: c.0.name.clone(),
				})
			}
			Expression::AccessSource(access) => {
				keys.push(access.column.name.text().to_string());
				projections.push(Projection::Group {
					column: access.column.name.text().to_string(),
					alias: access.column.name.clone(),
				})
			}

			expr => {
				return Err(error!(operation::aggregate_group_by_not_column(
					expr.full_fragment_owned()
				)));
			}
		}
	}

	let mut calls: Vec<SlotCall> = Vec::new();
	let mut digests = DigestSlots::default();
	let mut slots = Vec::new();
	for p in project {
		let (actual_expr, alias) = match p {
			Expression::Alias(alias_expr) => (alias_expr.expression.as_ref(), alias_expr.alias.0.clone()),
			expr => (expr, display_label(expr)),
		};

		let mut expression = actual_expr.clone();
		let reads = rewrite_aggregate_calls(
			&mut expression,
			&mut |node, origin| match node {
				Expression::Call(call) => Some(SlotCall {
					call: call.clone(),
					name: origin.unwrap_or(call).func.0.clone(),
				}),
				_ => None,
			},
			&mut calls,
			&mut digests,
		)
		.map_err(percentile_call_error)?;
		if reads.is_none_or(|reads| reads == 0) {
			return Err(error!(operation::aggregate_map_without_aggregate(
				actual_expr.full_fragment_owned()
			)));
		}
		for slot_call in &calls[slots.len()..] {
			slots.push(aggregate_slot(&slot_call.call, &slot_call.name, routines, ctx, &mut inputs)?);
		}
		projections.push(Projection::Computed {
			alias,
			expression,
		});
	}
	Ok((keys, projections, slots, inputs))
}

fn percentile_call_error(failure: PercentileCallError) -> Error {
	match failure {
		PercentileCallError::ArgumentCount {
			function,
			actual,
		} if actual > 3 => too_many_arguments(function, 3, actual),
		PercentileCallError::ArgumentCount {
			function,
			actual,
		} => RoutineError::FunctionArityMismatch {
			function,
			expected: 2,
			actual,
		}
		.into(),
		PercentileCallError::NotLiteral {
			argument,
			position,
		} => error!(operation::aggregate_argument_not_literal(argument, PERCENTILE_FUNCTION, position)),
		PercentileCallError::PercentileNotANumber {
			argument,
		} => error!(operation::aggregate_percentile_not_a_number(argument)),
		PercentileCallError::PercentileOutOfRange {
			argument,
		} => error!(operation::aggregate_percentile_out_of_range(argument)),
		PercentileCallError::Accuracy {
			argument,
			error: DigestError::AccuracyOutOfRange,
		} => error!(operation::aggregate_accuracy_out_of_range(argument)),
		PercentileCallError::Accuracy {
			argument,
			error: DigestError::AccuracyNotWholePpm,
		} => error!(operation::aggregate_accuracy_not_whole_ppm(argument)),
		PercentileCallError::Accuracy {
			argument,
			error: DigestError::AccuracyNotANumber,
		} => error!(operation::aggregate_accuracy_not_a_number(argument)),
		PercentileCallError::Accuracy {
			argument,
			error,
		} => RoutineError::FunctionExecutionFailed {
			function: argument,
			reason: error.to_string(),
		}
		.into(),
	}
}

fn aggregate_slot(
	call: &CallExpression,
	name: &Fragment,
	routines: &Routines,
	ctx: &QueryContext,
	inputs: &mut Vec<Expression>,
) -> Result<AggregateSlot> {
	let func_name = call.func.0.text();
	let function = routines.get_aggregate_function(func_name).ok_or_else(|| RoutineError::FunctionNotFound {
		function: call.func.0.clone(),
	})?;

	let literals = literal_arguments(call, function.max_literal_arguments())?;

	let mut fn_ctx = FunctionContext {
		fragment: name.clone(),
		identity: ctx.identity,
		row_count: 0,
		runtime_context: &ctx.services.runtime_context,
	};

	let accumulator =
		function.accumulator(&mut fn_ctx, &literals)?.ok_or_else(|| RoutineError::FunctionExecutionFailed {
			function: call.func.0.clone(),
			reason: format!("Function {} is not an aggregate", func_name),
		})?;

	let input = match call.args.first() {
		Some(Expression::Column(c)) => SlotInput::Column {
			name: c.0.name.text().to_string(),
			fragment: c.0.name.clone(),
		},
		Some(Expression::AccessSource(access)) => SlotInput::Column {
			name: access.column.name.text().to_string(),
			fragment: access.column.name.clone(),
		},
		Some(expression) => {
			inputs.push(expression.clone());
			SlotInput::Expression(inputs.len() - 1)
		}
		None => {
			return Err(RoutineError::FunctionArityMismatch {
				function: call.func.0.clone(),
				expected: 1,
				actual: 0,
			}
			.into());
		}
	};

	Ok(AggregateSlot {
		input,
		accumulator,
	})
}

fn literal_arguments(call: &CallExpression, max_literals: usize) -> Result<Vec<LiteralArgument>> {
	let func_name = call.func.0.text();
	let max_args = max_literals + 1;
	if call.args.len() > max_args {
		return Err(too_many_arguments(call.func.0.clone(), max_args, call.args.len()));
	}

	call.args
		.iter()
		.enumerate()
		.skip(1)
		.map(|(index, argument)| match argument {
			Expression::Constant(constant) => Ok(literal_argument(constant)),
			other => Err(error!(operation::aggregate_argument_not_literal(
				other.full_fragment_owned(),
				func_name,
				index + 1
			))),
		})
		.collect()
}

fn too_many_arguments(function: Fragment, max_args: usize, actual: usize) -> Error {
	let noun = if max_args == 1 {
		"argument"
	} else {
		"arguments"
	};
	TypeError::Function {
		kind: FunctionErrorKind::TooManyArguments {
			max_args,
			actual,
		},
		message: format!(
			"aggregate function {} takes at most {} {}, got {}",
			function.text(),
			max_args,
			noun,
			actual
		),
		fragment: function,
	}
	.into()
}

fn literal_argument(constant: &ConstantExpression) -> LiteralArgument {
	let (kind, fragment) = match constant {
		ConstantExpression::None {
			fragment,
		} => (LiteralKind::None, fragment),
		ConstantExpression::Bool {
			fragment,
		} => (LiteralKind::Bool, fragment),
		ConstantExpression::Number {
			fragment,
		} => (LiteralKind::Number, fragment),
		ConstantExpression::Text {
			fragment,
		} => (LiteralKind::Text, fragment),
		ConstantExpression::Temporal {
			fragment,
		} => (LiteralKind::Temporal, fragment),
		ConstantExpression::Duration {
			fragment,
		} => (LiteralKind::Duration, fragment),
	};
	LiteralArgument {
		kind,
		fragment: fragment.clone(),
	}
}

fn align_column_data(
	dict: &GroupKeyDict,
	produced: &[GroupId],
	data: (FieldRef, ArrayRef),
) -> Result<(FieldRef, ArrayRef)> {
	let mut position_of: Vec<Option<usize>> = vec![None; dict.len()];
	for (position, group) in produced.iter().enumerate() {
		if let Some(slot) = position_of.get_mut(group.index()) {
			*slot = Some(position);
		}
	}

	let reorder_indices: Vec<usize> = (0..dict.len())
		.map(|index| {
			position_of[index].ok_or_else(|| {
				CoreError::FrameError {
					message: format!(
						"Group key {:?} missing in aggregate output",
						dict.key_values(GroupId(index as u32))
					),
				}
				.into()
			})
		})
		.collect::<Result<Vec<_>>>()?;

	let aligned = take_rows(&batch(vec![data])?, &reorder_indices)?;
	Ok((aligned.schema_ref().fields()[0].clone(), aligned.column(0).clone()))
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		expression::{CallExpression, ColumnExpression, ConstantExpression, Expression, IdentExpression},
		interface::identifier::{ColumnIdentifier, ColumnObject},
	};
	use reifydb_routine_abi::{LiteralArgument, LiteralKind};
	use reifydb_value::fragment::{Fragment, StatementColumn, StatementLine};

	use super::literal_arguments;

	fn call(args: Vec<Expression>) -> CallExpression {
		CallExpression {
			func: IdentExpression(Fragment::internal("stats::digest")),
			args,
			fragment: Fragment::internal("stats::digest"),
		}
	}

	fn column(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			object: ColumnObject::Alias(Fragment::internal("t")),
			name: Fragment::internal(name),
		}))
	}

	fn positioned(text: &str, column: u32) -> Fragment {
		Fragment::Statement {
			text: Arc::from(text),
			line: StatementLine(1),
			column: StatementColumn(column),
		}
	}

	fn number(text: &str, column: u32) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: positioned(text, column),
		})
	}

	#[test]
	fn a_number_literal_reaches_the_accumulator_with_its_source_text_and_position() {
		// Accuracy must be parsed from the exact text, so a normalized 0.01 or a lost position is a bug.
		let literals = literal_arguments(&call(vec![column("x"), number("0.010", 24)]), 1)
			.expect("one literal must fit one literal slot");

		assert_eq!(
			literals,
			vec![LiteralArgument {
				kind: LiteralKind::Number,
				fragment: positioned("0.010", 24),
			}],
			"the literal must keep its kind, its exact text and its position"
		);
	}

	#[test]
	fn a_text_literal_is_passed_as_text_not_as_a_number() {
		// A quoted "0.01" passed as a number would be accepted as an accuracy the user never wrote as a number.
		let text = Expression::Constant(ConstantExpression::Text {
			fragment: positioned("0.01", 24),
		});

		let literals = literal_arguments(&call(vec![column("x"), text]), 1)
			.expect("one literal must fit one literal slot");

		assert_eq!(literals.len(), 1, "exactly one literal argument must be passed");
		assert_eq!(literals[0].kind, LiteralKind::Text, "a text literal must keep its text kind");
	}

	#[test]
	fn a_column_in_a_literal_position_reports_aggregate_010() {
		// A per-row accuracy cannot become one digest type, so a column there must fail instead of being read.
		let err = literal_arguments(&call(vec![column("x"), column("acc")]), 1).unwrap_err();

		assert_eq!(err.0.code, "AGGREGATE_010", "a column in a literal position must report AGGREGATE_010");
		assert_eq!(err.0.fragment.text(), "acc", "the error must point at the offending argument");
	}

	#[test]
	fn arguments_beyond_the_literal_slots_report_too_many_arguments_before_the_literal_check() {
		// Checking literals first would turn math::sum(a, b) from FUNCTION_003 into AGGREGATE_010.
		let over_one_slot =
			literal_arguments(&call(vec![column("x"), column("y"), column("z")]), 1).unwrap_err();
		let over_no_slot = literal_arguments(&call(vec![column("x"), column("y")]), 0).unwrap_err();

		assert_eq!(over_one_slot.0.code, "FUNCTION_003", "three arguments over one literal slot is too many");
		assert!(
			over_one_slot.0.message.contains("at most 2 arguments"),
			"the reported limit must count the literal slot, got: {}",
			over_one_slot.0.message
		);
		assert_eq!(over_no_slot.0.code, "FUNCTION_003", "a second argument with no literal slot is too many");
		assert!(
			over_no_slot.0.message.contains("at most 1 argument,"),
			"the limit message for plain aggregates must stay unchanged, got: {}",
			over_no_slot.0.message
		);
	}
}
