// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::{JoinType, WindowKind},
	interface::{
		catalog::flow::OperatorId,
		identifier::{ColumnIdentifier, ColumnObject},
	},
	value::column::columns::Columns,
};
use reifydb_rql::expression::{ColumnExpression, Expression};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, config::Config, error::Error, fragment::Fragment, value::duration::Duration};

use crate::{
	context::FlowContext,
	engine::{
		FlowEngineInner,
		register::{config::evaluate_operator_config, first_input},
	},
	error::FlowGraphError,
	operator::{
		aggregation::operator::AggregateOperator,
		append::AppendOperator,
		apply::ApplyOperator,
		distinct::operator::DistinctOperator,
		extend::ExtendOperator,
		filter::FilterOperator,
		gate::GateOperator,
		join::operator::{JoinOperator, JoinSideConfig},
		map::MapOperator,
		sort::SortOperator,
		take::TakeOperator,
		window::operator::{WindowConfig, WindowOperator},
	},
};

impl FlowEngineInner {
	#[inline]
	pub(super) fn add_filter(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		conditions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			Box::new(FilterOperator::new(
				parent_schema,
				operator_id,
				conditions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_gate(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		conditions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			Box::new(GateOperator::new(
				parent_schema,
				operator_id,
				conditions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_map(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			Box::new(MapOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_extend(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(
			operator_id,
			Box::new(ExtendOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_sort(&mut self, operator_id: OperatorId, inputs: &[OperatorId]) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(operator_id, Box::new(SortOperator::new(parent_schema, operator_id, Vec::new())));
		Ok(())
	}

	#[inline]
	pub(super) fn add_take(&mut self, operator_id: OperatorId, inputs: &[OperatorId], limit: usize) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		self.operators.insert(operator_id, Box::new(TakeOperator::new(parent_schema, operator_id, limit)));
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	pub(super) fn add_join(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		join_type: JoinType,
		left: Vec<Expression>,
		right: Vec<Expression>,
		alias: Option<String>,
		snapshot: bool,
		natural: bool,
		latest: bool,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		if inputs.len() != 2 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				operator: "Join",
				expected: "exactly 2",
				found: inputs.len(),
			}));
		}

		let left_node = inputs[0];
		let right_node = inputs[1];

		let left_schema = self
			.operators
			.get(&left_node)
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: "left parent".to_string(),
				})
			})?
			.output_schema()
			.unwrap_or_default();

		let right_schema = self
			.operators
			.get(&right_node)
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: "right parent".to_string(),
				})
			})?
			.output_schema()
			.expect("right side of join must have a statically known schema");

		let (left_exprs, right_exprs) = if natural {
			let common = common_column_names(&left_schema, &right_schema);
			let keys: Vec<Expression> = common.iter().map(|name| natural_key_expr(name)).collect();
			(keys.clone(), keys)
		} else {
			(left, right)
		};

		let join_lateness = self.catalog.find_operator_settings(txn, operator_id)?.and_then(|s| s.join);
		let left = join_lateness.as_ref().and_then(|j| j.left.as_ref());
		let left_lateness = left.map(|t| t.duration);
		let right = join_lateness.as_ref().and_then(|j| j.right.as_ref());
		let right_lateness = right.map(|t| t.duration);

		self.operators.insert(
			operator_id,
			Box::new(JoinOperator::new(
				JoinSideConfig {
					schema: left_schema,
					operator: left_node,
					exprs: left_exprs,
				},
				JoinSideConfig {
					schema: right_schema,
					operator: right_node,
					exprs: right_exprs,
				},
				operator_id,
				join_type,
				alias,
				self.routines.clone(),
				self.runtime_context.clone(),
				snapshot,
				natural,
				latest,
				left_lateness,
				right_lateness,
				Arc::clone(ctx),
			)),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_distinct(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		let lateness = self.operator_lateness(txn, operator_id)?;
		self.operators.insert(
			operator_id,
			Box::new(DistinctOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
				lateness,
			)),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_append(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
	) -> Result<()> {
		if inputs.len() < 2 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				operator: "Append",
				expected: "at least 2",
				found: inputs.len(),
			}));
		}

		let mut parent_schemas = Vec::with_capacity(inputs.len());

		for input_node_id in inputs {
			let schema = self
				.operators
				.get(input_node_id)
				.ok_or_else(|| {
					Error::from(FlowGraphError::ParentOperatorNotFound {
						input: format!("{:?}", input_node_id),
					})
				})?
				.output_schema();
			parent_schemas.push(schema);
		}

		let parent_schema = parent_schemas.swap_remove(0);
		let lateness = self.operator_lateness(txn, operator_id)?;
		self.operators.insert(
			operator_id,
			Box::new(AppendOperator::new(operator_id, parent_schema, inputs.to_vec(), lateness)),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_apply(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		operator: String,
		expressions: Vec<Expression>,
	) -> Result<()> {
		let config = evaluate_operator_config(expressions.as_slice(), &self.routines, &self.runtime_context)?;
		let cfg = Config::new(operator.as_str(), config);
		let lateness = self.operator_lateness(txn, operator_id)?;
		let parent_schema = self.parent_schema(first_input(inputs)?)?;

		let provider = self.operator_provider.clone();
		let inner = provider.provide(operator_id, &cfg)?;

		self.operators
			.insert(operator_id, Box::new(ApplyOperator::new(parent_schema, operator_id, inner, lateness)));
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	pub(super) fn add_window(
		&mut self,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		kind: WindowKind,
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		lateness: Duration,
		immutable: Option<Duration>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		let operator = WindowOperator::new(WindowConfig {
			parent_schema,
			operator: operator_id,
			kind: kind.clone(),
			group_by: group_by.clone(),
			aggregations: aggregations.clone(),
			runtime_context: self.runtime_context.clone(),
			routines: self.routines.clone(),
			lateness,
			immutable,
			ctx: Arc::clone(ctx),
		});
		self.operators.insert(operator_id, Box::new(operator));
		Ok(())
	}

	#[inline]
	pub(super) fn add_aggregate(
		&mut self,
		txn: &mut Transaction<'_>,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		by: Vec<Expression>,
		map: Vec<Expression>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(first_input(inputs)?)?;
		let operator = AggregateOperator::new(
			parent_schema,
			operator_id,
			by,
			map,
			self.routines.clone(),
			self.runtime_context.clone(),
			self.operator_lateness(txn, operator_id)?,
		);
		self.operators.insert(operator_id, Box::new(operator));
		Ok(())
	}
}

fn common_column_names(left: &Columns, right: &Columns) -> Vec<String> {
	let right_names: Vec<String> = right.names.iter().map(|n| n.text().to_string()).collect();
	left.names.iter().map(|n| n.text().to_string()).filter(|name| right_names.contains(name)).collect()
}

fn natural_key_expr(name: &str) -> Expression {
	Expression::Column(ColumnExpression(ColumnIdentifier {
		object: ColumnObject::Qualified {
			namespace: Fragment::internal("_context"),
			name: Fragment::internal("_context"),
		},
		name: Fragment::internal(name),
	}))
}
