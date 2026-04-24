// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use reifydb_core::{
	error::CoreError,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_routine::function::{Accumulator, FunctionContext, error::FunctionError, registry::Functions};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

enum Projection {
	Aggregate {
		column: String,
		alias: Fragment,
		accumulator: Box<dyn Accumulator>,
	},
	Group {
		column: String,
		alias: Fragment,
	},
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
}

impl QueryNode for AggregateNode {
	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		// Already has context from constructor
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::aggregate::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "AggregateNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let (keys, mut projections) =
			parse_keys_and_aggregates(&self.by, &self.map, &stored_ctx.services.functions, stored_ctx)?;

		let mut seen_groups = HashSet::<Vec<Value>>::new();
		let mut group_key_order: Vec<Vec<Value>> = Vec::new();

		while let Some(columns) = self.input.next(rx, ctx)? {
			let groups = columns.group_by_view(&keys)?;

			for (group_key, _) in &groups {
				if seen_groups.insert(group_key.clone()) {
					group_key_order.push(group_key.clone());
				}
			}

			for projection in &mut projections {
				if let Projection::Aggregate {
					accumulator,
					column,
					..
				} = projection
				{
					let column_ref = columns.column(column).unwrap();
					let cwn = ColumnWithName::new(column_ref.name().clone(), column_ref.data().clone());
					accumulator.update(&Columns::new(vec![cwn]), &groups).unwrap();
				}
			}
		}

		let mut result_columns = Vec::new();

		for projection in projections {
			match projection {
				Projection::Group {
					alias,
					column,
					..
				} => {
					let col_idx = keys.iter().position(|k| k == &column).unwrap();

					let first_key_type = if group_key_order.is_empty() {
						None
					} else {
						Some(group_key_order[0][col_idx].get_type())
					};
					let mut c = ColumnWithName {
						name: Fragment::internal(alias.fragment()),
						data: ColumnBuffer::none_typed(
							first_key_type.unwrap_or(Type::Boolean),
							0,
						),
					};
					for key in &group_key_order {
						c.data_mut().push_value(key[col_idx].clone());
					}
					result_columns.push(c);
				}
				Projection::Aggregate {
					alias,
					mut accumulator,
					..
				} => {
					let (keys_out, mut data) = accumulator.finalize().unwrap();
					align_column_data(&group_key_order, &keys_out, &mut data).unwrap();
					result_columns.push(ColumnWithName {
						name: Fragment::internal(alias.fragment()),
						data,
					});
				}
			}
		}

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
	functions: &'a Functions,
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
				// Handle qualified column references like
				// departments.dept_name
				keys.push(access.column.name.text());
				projections.push(Projection::Group {
					column: access.column.name.text().to_string(),
					alias: access.column.name.clone(),
				})
			}
			// _ => return
			// Err(reifydb_type::error::Error::Unsupported("Non-column
			// group by not supported".into())),
			expr => panic!("Non-column group by not supported: {expr:#?}"),
		}
	}

	for p in project {
		// Extract the actual expression, handling aliases
		let (actual_expr, alias) = match p {
			Expression::Alias(alias_expr) => {
				// This is an aliased expression like
				// "total_count: count(value)"
				(alias_expr.expression.as_ref(), alias_expr.alias.0.clone())
			}
			expr => {
				// Non-aliased expression, use the expression's
				// fragment as alias
				(expr, expr.full_fragment_owned())
			}
		};

		match actual_expr {
			Expression::Call(call) => {
				let func_name = call.func.0.text();
				let function =
					functions.get_aggregate(func_name).ok_or_else(|| FunctionError::NotFound {
						function: call.func.0.clone(),
					})?;

				let fn_ctx = FunctionContext::new(
					call.func.0.clone(),
					&ctx.services.runtime_context,
					ctx.identity,
					0,
				);

				let accumulator = function.accumulator(&fn_ctx).ok_or_else(|| {
					FunctionError::ExecutionFailed {
						function: call.func.0.clone(),
						reason: format!("Function {} is not an aggregate", func_name),
					}
				})?;

				match call.args.first() {
					Some(Expression::Column(c)) => {
						projections.push(Projection::Aggregate {
							column: c.0.name.text().to_string(),
							alias,
							accumulator,
						});
					}
					Some(Expression::AccessSource(access)) => {
						// Handle qualified column
						// references in aggregate
						// functions
						projections.push(Projection::Aggregate {
							column: access.column.name.text().to_string(),
							alias,
							accumulator,
						});
					}
					None => {
						return Err(FunctionError::ArityMismatch {
							function: call.func.0.clone(),
							expected: 1,
							actual: 0,
						}
						.into());
					}
					Some(arg) => {
						let actual_type = arg.infer_type().ok_or_else(|| {
							FunctionError::ExecutionFailed {
								function: call.func.0.clone(),
								reason: "aggregate function arguments must be column references".to_string(),
							}
						})?;
						let expected = function.accepted_types().expected_at(0).to_vec();
						return Err(FunctionError::InvalidArgumentType {
							function: call.func.0.clone(),
							argument_index: 0,
							expected,
							actual: actual_type,
						}
						.into());
					}
				}
			}
			// _ => return
			// Err(reifydb_type::error::Error::Unsupported("Expected
			// aggregate call expression".into())),
			_ => panic!("Expected aggregate call expression, got: {actual_expr:#?}"),
		}
	}
	Ok((keys, projections))
}

fn align_column_data(group_key_order: &[Vec<Value>], keys: &[Vec<Value>], data: &mut ColumnBuffer) -> Result<()> {
	let mut key_to_index = HashMap::new();
	for (i, key) in keys.iter().enumerate() {
		key_to_index.insert(key, i);
	}

	let reorder_indices: Vec<usize> = group_key_order
		.iter()
		.map(|k| {
			key_to_index.get(k).copied().ok_or_else(|| {
				CoreError::FrameError {
					message: format!("Group key {:?} missing in aggregate output", k),
				}
				.into()
			})
		})
		.collect::<Result<Vec<_>>>()?;

	data.reorder(&reorder_indices);
	Ok(())
}
