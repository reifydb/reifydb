// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_schema::Schema;
use reifydb_codec::row::shape::RowFamily;
use reifydb_core::{
	common::JoinType,
	error::diagnostic::operation::{lookup_key_type_mismatch, lookup_right_unsupported},
	expression::{ColumnExpression, Expression},
	flow::{dag::FlowDag, operator::LookupObject},
	interface::{
		catalog::{
			column::Column,
			flow::{FlowId, OperatorId},
			object::ObjectId,
			storage::StorageId,
			view::{View, ViewKind},
		},
		identifier::{ColumnIdentifier, ColumnObject},
	},
	operator_with::{AggregateWith, ApplyWith, DistinctWith, JoinWith, LookupWith, WindowWith},
	row::row_shape_from_columns,
};
use reifydb_flow::{
	context::FlowContext,
	error::FlowGraphError,
	operator::{
		append::{AppendOperator, lane::assign_lanes},
		extend::ExtendOperator,
		filter::FilterOperator,
		map::MapOperator,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	config::ExtensionParams,
	error,
	error::Error,
	fragment::Fragment,
	value::value_type::{ValueType, field::from_field},
};

use crate::{
	engine::{
		FlowEngineInner,
		register::{config::evaluate_operator_params, first_input},
	},
	operator::{
		aggregation::operator::AggregateOperator,
		apply::ApplyOperator,
		distinct::operator::DistinctOperator,
		gate::GateOperator,
		join::operator::{JoinOperator, JoinSideConfig},
		lookup::{LookupConfig, LookupOperator},
		scan::catalog_schema,
		sort::SortOperator,
		take::TakeOperator,
		window::operator::{WindowConfig, WindowOperator},
	},
};

impl FlowEngineInner {
	#[inline]
	pub(super) fn add_filter(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		conditions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		self.operators.insert(
			(flow_id, operator_id),
			Box::new(FilterOperator::new(
				parent_schema,
				operator_id,
				conditions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)?),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_gate(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		conditions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		self.operators.insert(
			(flow_id, operator_id),
			Box::new(GateOperator::new(
				parent_schema,
				operator_id,
				conditions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)?),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_map(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		self.operators.insert(
			(flow_id, operator_id),
			Box::new(MapOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)?),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_extend(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		self.operators.insert(
			(flow_id, operator_id),
			Box::new(ExtendOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)?),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_sort(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		self.operators.insert(
			(flow_id, operator_id),
			Box::new(SortOperator::new(parent_schema, operator_id, Vec::new())),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_take(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		limit: usize,
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		self.operators
			.insert((flow_id, operator_id), Box::new(TakeOperator::new(parent_schema, operator_id, limit)));
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	pub(super) fn add_join(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		join_type: JoinType,
		left: Vec<Expression>,
		right: Vec<Expression>,
		alias: Option<String>,
		natural: bool,
		with: JoinWith,
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
			.get(&(flow_id, left_node))
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: "left parent".to_string(),
				})
			})?
			.output_schema()
			.unwrap_or_else(|| Arc::new(Schema::empty()));

		let right_schema = self
			.operators
			.get(&(flow_id, right_node))
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

		let left_retention = with.retention.as_ref().and_then(|j| j.left.as_ref()).map(|t| t.duration);
		let right_retention = with.retention.as_ref().and_then(|j| j.right.as_ref()).map(|t| t.duration);

		self.operators.insert(
			(flow_id, operator_id),
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
				with.snapshot,
				natural,
				with.pick,
				left_retention,
				right_retention,
				Arc::clone(ctx),
			)?),
		);
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	pub(super) fn add_lookup(
		&mut self,
		txn: &mut Transaction<'_>,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		join_type: JoinType,
		right: LookupObject,
		left: Vec<Expression>,
		alias: Option<String>,
		with: LookupWith,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		if inputs.len() != 1 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				operator: "Lookup",
				expected: "exactly 1",
				found: inputs.len(),
			}));
		}

		let left_node = inputs[0];
		let left_schema = self
			.operators
			.get(&(flow_id, left_node))
			.ok_or_else(|| {
				Error::from(FlowGraphError::ParentOperatorNotFound {
					input: "left parent".to_string(),
				})
			})?
			.output_schema()
			.unwrap_or_else(|| Arc::new(Schema::empty()));

		let (storage, deferred, columns, partition_by) = match right {
			LookupObject::Table(table) => {
				let table = self.catalog.get_table(&mut txn.reborrow(), table)?;
				(StorageId::Table(table.id), false, table.columns, table.partition_by)
			}
			LookupObject::View(view) => match self.catalog.get_view(&mut txn.reborrow(), view)? {
				View::Table(view) if view.sort.is_empty() => (
					StorageId::View(view.id),
					view.kind == ViewKind::Deferred,
					view.columns,
					view.partition_by,
				),
				other => {
					return Err(error!(lookup_right_unsupported(Fragment::None, other.name())));
				}
			},
		};

		ensure_lookup_key_types(&left, &left_schema, &columns, &partition_by)?;

		let shape = row_shape_from_columns(RowFamily::Table, &columns);
		let right_schema = catalog_schema(&columns);
		let left_retention = with.retention.as_ref().map(|retention| retention.duration);

		let operator = LookupOperator::new(LookupConfig {
			operator: operator_id,
			left_node,
			join_type,
			right,
			deferred,
			storage,
			columns,
			partition_by,
			shape,
			left,
			left_schema,
			right_schema,
			alias,
			left_retention,
			routines: self.routines.clone(),
			runtime_context: self.runtime_context.clone(),
			ctx: Arc::clone(ctx),
		})?;
		self.operators.insert((flow_id, operator_id), Box::new(operator));
		if let (LookupObject::View(view), true) = (right, deferred) {
			self.add_lookup_source(flow_id, operator_id, ObjectId::view(view));
		}
		Ok(())
	}

	#[inline]
	pub(super) fn add_distinct(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		expressions: Vec<Expression>,
		_with: DistinctWith,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		self.operators.insert(
			(flow_id, operator_id),
			Box::new(DistinctOperator::new(
				parent_schema,
				operator_id,
				expressions,
				self.routines.clone(),
				self.runtime_context.clone(),
				Arc::clone(ctx),
			)?),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_append(
		&mut self,
		flow: &FlowDag,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
	) -> Result<()> {
		if inputs.len() != 2 {
			return Err(Error::from(FlowGraphError::NodeInputArity {
				operator: "Append",
				expected: "exactly 2",
				found: inputs.len(),
			}));
		}

		let mut parent_schemas = Vec::with_capacity(inputs.len());

		for input_node_id in inputs {
			let schema = self
				.operators
				.get(&(flow_id, *input_node_id))
				.ok_or_else(|| {
					Error::from(FlowGraphError::ParentOperatorNotFound {
						input: format!("{:?}", input_node_id),
					})
				})?
				.output_schema();
			parent_schemas.push(schema);
		}

		let parent_schema = parent_schemas.swap_remove(0);
		let lanes = assign_lanes(flow, operator_id)?;
		self.operators.insert(
			(flow_id, operator_id),
			Box::new(AppendOperator::new(operator_id, parent_schema, inputs.to_vec(), lanes)),
		);
		Ok(())
	}

	#[inline]
	pub(super) fn add_apply(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		operator: String,
		params: Vec<Expression>,
		with: ApplyWith,
	) -> Result<()> {
		let values = evaluate_operator_params(params.as_slice(), &self.routines, &self.runtime_context)?;
		let params = ExtensionParams::new(operator.as_str(), values);
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;

		let provider = self.operator_provider.clone();
		let inner = provider.provide(operator_id, &params, &with)?;

		self.operators.insert(
			(flow_id, operator_id),
			Box::new(ApplyOperator::new(parent_schema, operator_id, inner, &with)),
		);
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	pub(super) fn add_window(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		group_by: Vec<Expression>,
		aggregations: Vec<Expression>,
		with: WindowWith,
		ctx: &Arc<FlowContext>,
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		let operator = WindowOperator::new(WindowConfig {
			parent_schema,
			operator: operator_id,
			kind: with.kind,
			group_by,
			aggregations,
			runtime_context: self.runtime_context.clone(),
			routines: self.routines.clone(),
			lateness: with.lateness,
			immutable: with.immutable,
			ctx: Arc::clone(ctx),
		})?;
		self.operators.insert((flow_id, operator_id), Box::new(operator));
		Ok(())
	}

	#[inline]
	pub(super) fn add_aggregate(
		&mut self,
		flow_id: FlowId,
		operator_id: OperatorId,
		inputs: &[OperatorId],
		by: Vec<Expression>,
		map: Vec<Expression>,
		_with: AggregateWith,
	) -> Result<()> {
		let parent_schema = self.parent_schema(flow_id, first_input(inputs)?)?;
		let operator = AggregateOperator::new(
			parent_schema,
			operator_id,
			by,
			map,
			self.routines.clone(),
			self.runtime_context.clone(),
		)?;
		self.operators.insert((flow_id, operator_id), Box::new(operator));
		Ok(())
	}
}

fn ensure_lookup_key_types(
	left: &[Expression],
	left_schema: &Schema,
	columns: &[Column],
	partition_by: &[String],
) -> Result<()> {
	for (key, name) in left.iter().zip(partition_by) {
		let Some(right_column) = columns.iter().find(|column| &column.name == name) else {
			continue;
		};
		let Some(left_type) = left_key_type(key, left_schema)? else {
			continue;
		};
		let right_type = right_column.constraint.get_type();
		if !lookup_key_types_compatible(left_type.inner_type(), right_type.inner_type()) {
			return Err(error!(lookup_key_type_mismatch(
				key.full_fragment_owned(),
				left_type.inner_type().clone(),
				right_type.inner_type().clone()
			)));
		}
	}
	Ok(())
}

fn left_key_type(key: &Expression, left_schema: &Schema) -> Result<Option<ValueType>> {
	let name = match key {
		Expression::Column(ColumnExpression(column)) => column.name.text(),
		Expression::AccessSource(access) => access.column.name.text(),
		_ => return Ok(None),
	};
	let Some(field) = left_schema.fields().iter().find(|field| field.name() == name) else {
		return Ok(None);
	};
	Ok(from_field(field)?.value_type)
}

fn lookup_key_types_compatible(left: &ValueType, right: &ValueType) -> bool {
	left == right || (left.is_number() && right.is_number())
}

fn common_column_names(left: &Schema, right: &Schema) -> Vec<String> {
	let right_names: Vec<String> = right.fields().iter().map(|field| field.name().clone()).collect();
	left.fields().iter().map(|field| field.name().clone()).filter(|name| right_names.contains(name)).collect()
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
