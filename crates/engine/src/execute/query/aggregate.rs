// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use async_trait::async_trait;
use reifydb_core::value::column::{Column, ColumnData, Columns, headers::ColumnHeaders};
use reifydb_rql::expression::Expression;
use reifydb_type::{Fragment, Value, diagnostic};
use tracing::instrument;

use crate::{
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
	function::{AggregateFunction, AggregateFunctionContext, Functions},
};

enum Projection {
	Aggregate {
		column: String,
		alias: Fragment,
		function: Box<dyn AggregateFunction>,
	},
	Group {
		column: String,
		alias: Fragment,
	},
}

pub(crate) struct AggregateNode {
	input: Box<ExecutionPlan>,
	by: Vec<Expression>,
	map: Vec<Expression>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<ExecutionContext>>,
}

impl AggregateNode {
	pub fn new(
		input: Box<ExecutionPlan>,
		by: Vec<Expression>,
		map: Vec<Expression>,
		context: Arc<ExecutionContext>,
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

#[async_trait]
impl QueryNode for AggregateNode {
	#[instrument(level = "trace", skip_all, name = "query::aggregate::initialize")]
	async fn initialize<'a>(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		self.input.initialize(rx, ctx).await?;
		// Already has context from constructor
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "query::aggregate::next")]
	async fn next<'a>(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "AggregateNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.headers.is_some() {
			return Ok(None);
		}

		let (keys, mut projections) =
			parse_keys_and_aggregates(&self.by, &self.map, &stored_ctx.executor.functions)?;

		let mut seen_groups = HashSet::<Vec<Value>>::new();
		let mut group_key_order: Vec<Vec<Value>> = Vec::new();

		while let Some(Batch {
			columns,
		}) = self.input.next(rx, ctx).await?
		{
			let groups = columns.group_by_view(&keys)?;

			for (group_key, _) in &groups {
				if seen_groups.insert(group_key.clone()) {
					group_key_order.push(group_key.clone());
				}
			}

			for projection in &mut projections {
				if let Projection::Aggregate {
					function,
					column,
					..
				} = projection
				{
					let column = columns.column(column).unwrap();
					function.aggregate(AggregateFunctionContext {
						column,
						groups: &groups,
					})
					.unwrap();
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

					let mut c = Column {
						name: Fragment::internal(alias.fragment()),
						data: ColumnData::undefined(0),
					};
					for key in &group_key_order {
						c.data_mut().push_value(key[col_idx].clone());
					}
					result_columns.push(c);
				}
				Projection::Aggregate {
					alias,
					mut function,
					..
				} => {
					let (keys_out, mut data) = function.finalize().unwrap();
					align_column_data(&group_key_order, &keys_out, &mut data).unwrap();
					result_columns.push(Column {
						name: Fragment::internal(alias.fragment()),
						data,
					});
				}
			}
		}

		let columns = Columns::new(result_columns);
		self.headers = Some(ColumnHeaders::from_columns(&columns));

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}

fn parse_keys_and_aggregates<'a>(
	by: &'a [Expression],
	project: &'a [Expression],
	functions: &'a Functions,
) -> crate::Result<(Vec<&'a str>, Vec<Projection>)> {
	let mut keys = Vec::new();
	let mut projections = Vec::new();

	for gb in by {
		match gb {
			Expression::Column(c) => {
				keys.push(c.0.name.text());
				projections.push(Projection::Group {
					column: c.0.name.text().to_string(),
					alias: c.0.name.clone().into_owned(),
				})
			}
			Expression::AccessSource(access) => {
				// Handle qualified column references like
				// departments.dept_name
				keys.push(access.column.name.text());
				projections.push(Projection::Group {
					column: access.column.name.text().to_string(),
					alias: access.column.name.clone().into_owned(),
				})
			}
			// _ => return
			// Err(reifydb_type::Error::Unsupported("Non-column
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
				let func = call.func.0.text();
				match call.args.first().map(|arg| arg) {
					Some(Expression::Column(c)) => {
						let function = functions.get_aggregate(func).unwrap();
						projections.push(Projection::Aggregate {
							column: c.0.name.text().to_string(),
							alias: alias.into_owned(),
							function,
						});
					}
					Some(Expression::AccessSource(access)) => {
						// Handle qualified column
						// references in aggregate
						// functions
						let function = functions.get_aggregate(func).unwrap();
						projections.push(Projection::Aggregate {
							column: access.column.name.text().to_string(),
							alias: alias.into_owned(),
							function,
						});
					}
					// _ => return
					// Err(reifydb_type::Error::Unsupported("
					// Aggregate args must be
					// columns".into())),
					_ => panic!("Aggregate args must be columns"),
				}
			}
			// _ => return
			// Err(reifydb_type::Error::Unsupported("Expected
			// aggregate call expression".into())),
			_ => panic!("Expected aggregate call expression, got: {actual_expr:#?}"),
		}
	}
	Ok((keys, projections))
}

fn align_column_data(group_key_order: &[Vec<Value>], keys: &[Vec<Value>], data: &mut ColumnData) -> crate::Result<()> {
	let mut key_to_index = HashMap::new();
	for (i, key) in keys.iter().enumerate() {
		key_to_index.insert(key, i);
	}

	let reorder_indices: Vec<usize> = group_key_order
		.iter()
		.map(|k| {
			key_to_index.get(k).copied().ok_or_else(|| {
				reifydb_type::error!(diagnostic::engine::frame_error(format!(
					"Group key {:?} missing in aggregate output",
					k
				)))
			})
		})
		.collect::<crate::Result<Vec<_>>>()?;

	data.reorder(&reorder_indices);
	Ok(())
}
