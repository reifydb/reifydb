// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{slice, sync::Arc};

use reifydb_core::{
	error::{
		CoreError,
		diagnostic::{operation, query},
	},
	metrics::heap::HeapSize,
	value::column::{
		ColumnWithName,
		buffer::ColumnBuffer,
		columns::Columns,
		headers::ColumnHeaders,
		view::group_by::{GroupId, GroupKeyDict, GroupRows},
	},
};
use reifydb_evaluate::expression::{compile::compile_expression, context::CompileContext};
use reifydb_routine_abi::{
	Accumulator, FunctionKind, context::FunctionContext, error::RoutineError, registry::Routines,
};
use reifydb_rql::{
	expression::{CallExpression, Expression, name::display_label},
	flow::aggregate::{rewrite_aggregate_calls, synthetic_aggregate_column_name},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	error::{FunctionErrorKind, TypeError},
	fragment::Fragment,
	reifydb_assertions,
	value::value_type::ValueType,
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{
		QueryContext, QueryNode, charge_query_memory_bytes, eval_context_from_query, is_scalar_type,
	},
};

struct AggregateSlot {
	column: String,
	column_fragment: Fragment,
	accumulator: Box<dyn Accumulator>,
}

impl AggregateSlot {
	fn update(&mut self, columns: &Columns, groups: &GroupRows) -> Result<()> {
		let column_ref = columns
			.column(&self.column)
			.ok_or_else(|| error!(query::column_not_found(self.column_fragment.clone())))?;
		let cwn = ColumnWithName::new(column_ref.name().clone(), column_ref.data().clone());
		self.accumulator.update(&Columns::new(vec![cwn]), groups)?;
		Ok(())
	}

	fn finalize(mut self, dict: &GroupKeyDict) -> Result<ColumnBuffer> {
		let (keys_out, mut data) = self.accumulator.finalize()?;
		align_column_data(dict, &keys_out, &mut data)?;
		Ok(data)
	}
}

enum Projection {
	Aggregate {
		alias: Fragment,
		slot: AggregateSlot,
	},
	Group {
		column: String,
		alias: Fragment,
	},
	Computed {
		alias: Fragment,
		expression: Expression,
		slots: Vec<AggregateSlot>,
	},
}

impl Projection {
	fn slots(&self) -> &[AggregateSlot] {
		match self {
			Projection::Aggregate {
				slot,
				..
			} => slice::from_ref(slot),
			Projection::Group {
				..
			} => &[],
			Projection::Computed {
				slots,
				..
			} => slots,
		}
	}

	fn slots_mut(&mut self) -> &mut [AggregateSlot] {
		match self {
			Projection::Aggregate {
				slot,
				..
			} => slice::from_mut(slot),
			Projection::Group {
				..
			} => &mut [],
			Projection::Computed {
				slots,
				..
			} => slots,
		}
	}
}

pub(crate) struct AggregateNode {
	input: Box<dyn QueryNode>,
	by: Vec<Expression>,
	map: Vec<Expression>,
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
			headers: None,
			context: Some(context),
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::accumulate")]
	fn accumulate<'a>(
		input: &mut Box<dyn QueryNode>,
		rx: &mut Transaction<'a>,
		ctx: &mut QueryContext,
		keys: &[&str],
		projections: &mut [Projection],
		dict: &mut GroupKeyDict,
		key_types: &mut Vec<Option<ValueType>>,
	) -> Result<()> {
		let mut charged = 0usize;
		while let Some(columns) = input.next(rx, ctx)? {
			if key_types.is_empty() {
				key_types.extend(keys
					.iter()
					.map(|key| columns.column(key).map(|c| c.data().get_type())));
				let aliases = projections.iter().filter_map(|projection| match projection {
					Projection::Group {
						alias,
						..
					} => Some(alias),
					Projection::Aggregate {
						..
					}
					| Projection::Computed {
						..
					} => None,
				});
				for (key_type, alias) in key_types.iter().zip(aliases) {
					if let Some(ty) = key_type
						&& !is_scalar_type(ty)
					{
						return Err(error!(operation::aggregate_group_by_unkeyable(
							alias.clone(),
							ty.clone()
						)));
					}
				}
			}
			let groups = columns.group_by_ids(keys, dict)?;

			for projection in projections.iter_mut() {
				for slot in projection.slots_mut() {
					slot.update(&columns, &groups)?;
				}
			}

			let state = dict.heap_size()
				+ projections
					.iter()
					.flat_map(Projection::slots)
					.map(|slot| slot.accumulator.heap_size())
					.sum::<usize>();
			charge_query_memory_bytes(&ctx.memory, &mut charged, state)?;
		}

		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::finalize")]
	fn finalize(
		projections: Vec<Projection>,
		keys: &[&str],
		dict: &GroupKeyDict,
		key_types: &[Option<ValueType>],
		ctx: &QueryContext,
	) -> Result<Vec<ColumnWithName>> {
		let mut result_columns = Vec::new();

		for projection in projections {
			match projection {
				Projection::Group {
					alias,
					column,
					..
				} => {
					let col_idx = keys.iter().position(|k| k == &column).unwrap();

					let first_key_type = dict.values(GroupId(0)).map(|key| key[col_idx].get_type());
					let key_type = first_key_type
						.or_else(|| key_types.get(col_idx).cloned().flatten())
						.unwrap_or(ValueType::Boolean);
					let mut c = ColumnWithName {
						name: Fragment::internal(alias.fragment()),
						data: ColumnBuffer::none_typed(key_type, 0),
					};
					for (_, key) in dict.iter() {
						c.data_mut().push_value(key[col_idx].clone());
					}
					result_columns.push(c);
				}
				Projection::Aggregate {
					alias,
					slot,
				} => {
					result_columns.push(ColumnWithName {
						name: Fragment::internal(alias.fragment()),
						data: slot.finalize(dict)?,
					});
				}
				Projection::Computed {
					alias,
					expression,
					slots,
				} => {
					let slot_columns = slots
						.into_iter()
						.enumerate()
						.map(|(idx, slot)| {
							Ok(ColumnWithName::new(
								Fragment::internal(synthetic_aggregate_column_name(
									idx,
								)),
								slot.finalize(dict)?,
							))
						})
						.collect::<Result<Vec<_>>>()?;
					let compiled = compile_expression(
						&CompileContext {
							symbols: &ctx.symbols,
						},
						&expression,
					)?;
					let eval_ctx = eval_context_from_query(ctx)
						.with_eval(Columns::new(slot_columns), dict.len());
					result_columns.push(ColumnWithName {
						name: Fragment::internal(alias.fragment()),
						data: compiled.execute(&eval_ctx)?.data,
					});
				}
			}
		}

		Ok(result_columns)
	}
}

impl QueryNode for AggregateNode {
	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;

		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "AggregateNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let (keys, mut projections) =
			parse_keys_and_aggregates(&self.by, &self.map, &stored_ctx.services.routines, stored_ctx)?;

		let mut dict = GroupKeyDict::new();
		let mut key_types = Vec::new();

		Self::accumulate(&mut self.input, rx, ctx, &keys, &mut projections, &mut dict, &mut key_types)?;

		let result_columns = Self::finalize(projections, &keys, &dict, &key_types, stored_ctx)?;

		let columns = Columns::new(result_columns);
		self.headers = Some(ColumnHeaders::from_columns(&columns));

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}

fn parse_keys_and_aggregates<'a>(
	by: &'a [Expression],
	project: &'a [Expression],
	routines: &'a Routines,
	ctx: &QueryContext,
) -> Result<(Vec<&'a str>, Vec<Projection>)> {
	let mut keys = Vec::new();
	let mut projections = Vec::new();

	for gb in by {
		match gb {
			Expression::Column(c) => {
				keys.push(c.0.name.text());
				projections.push(Projection::Group {
					column: c.0.name.text().to_string(),
					alias: c.0.name.clone(),
				})
			}
			Expression::AccessSource(access) => {
				keys.push(access.column.name.text());
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

	for p in project {
		let (actual_expr, alias) = match p {
			Expression::Alias(alias_expr) => (alias_expr.expression.as_ref(), alias_expr.alias.0.clone()),
			expr => (expr, display_label(expr)),
		};

		match actual_expr {
			Expression::Call(call) => {
				let slot = aggregate_slot(call, routines, ctx)?;
				projections.push(Projection::Aggregate {
					alias,
					slot,
				});
			}

			expr => {
				let mut expression = expr.clone();
				let mut calls = Vec::new();
				let rewritten = rewrite_aggregate_calls(
					&mut expression,
					&mut |e| match e {
						Expression::Call(call) => Some(call.clone()),
						_ => None,
					},
					&mut calls,
				);
				if !rewritten || calls.is_empty() {
					return Err(error!(operation::aggregate_map_without_aggregate(
						expr.full_fragment_owned()
					)));
				}
				let slots = calls
					.iter()
					.map(|call| aggregate_slot(call, routines, ctx))
					.collect::<Result<Vec<_>>>()?;
				projections.push(Projection::Computed {
					alias,
					expression,
					slots,
				});
			}
		}
	}
	Ok((keys, projections))
}

fn aggregate_slot(call: &CallExpression, routines: &Routines, ctx: &QueryContext) -> Result<AggregateSlot> {
	let func_name = call.func.0.text();
	let function = routines.get_aggregate_function(func_name).ok_or_else(|| RoutineError::FunctionNotFound {
		function: call.func.0.clone(),
	})?;
	let _ = FunctionKind::Aggregate;

	let mut fn_ctx = FunctionContext {
		fragment: call.func.0.clone(),
		identity: ctx.identity,
		row_count: 0,
		runtime_context: &ctx.services.runtime_context,
	};

	let accumulator = function.accumulator(&mut fn_ctx).ok_or_else(|| RoutineError::FunctionExecutionFailed {
		function: call.func.0.clone(),
		reason: format!("Function {} is not an aggregate", func_name),
	})?;

	if call.args.len() > 1 {
		return Err(TypeError::Function {
			kind: FunctionErrorKind::TooManyArguments {
				max_args: 1,
				actual: call.args.len(),
			},
			message: format!(
				"aggregate function {} takes at most 1 argument, got {}",
				func_name,
				call.args.len()
			),
			fragment: call.func.0.clone(),
		}
		.into());
	}

	match call.args.first() {
		Some(Expression::Column(c)) => Ok(AggregateSlot {
			column: c.0.name.text().to_string(),
			column_fragment: c.0.name.clone(),
			accumulator,
		}),
		Some(Expression::AccessSource(access)) => Ok(AggregateSlot {
			column: access.column.name.text().to_string(),
			column_fragment: access.column.name.clone(),
			accumulator,
		}),
		None => Err(RoutineError::FunctionArityMismatch {
			function: call.func.0.clone(),
			expected: 1,
			actual: 0,
		}
		.into()),
		Some(arg) => {
			let actual_type = arg.infer_type().ok_or_else(|| RoutineError::FunctionExecutionFailed {
				function: call.func.0.clone(),
				reason: "aggregate function arguments must be column references".to_string(),
			})?;
			let expected = function.accepted_types().expected_at(0).to_vec();
			Err(RoutineError::FunctionInvalidArgumentType {
				function: call.func.0.clone(),
				argument_index: 0,
				expected,
				actual: actual_type,
			}
			.into())
		}
	}
}

fn align_column_data(dict: &GroupKeyDict, produced: &[GroupId], data: &mut ColumnBuffer) -> Result<()> {
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
						dict.values(GroupId(index as u32))
					),
				}
				.into()
			})
		})
		.collect::<Result<Vec<_>>>()?;

	data.reorder(&reorder_indices);
	Ok(())
}
