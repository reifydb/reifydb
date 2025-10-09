// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Conversion utilities for transforming borrowed data to owned data at the
//! flow compilation boundary

use reifydb_type::Fragment;

use crate::{
	expression::{
		AccessSourceExpression, AddExpression, AliasExpression, AndExpression, BetweenExpression,
		CallExpression, CastExpression, ColumnExpression, ConstantExpression, DivExpression, ElseIfExpression,
		EqExpression, Expression, ExtendExpression, GreaterThanEqExpression, GreaterThanExpression,
		IdentExpression, IfExpression, LessThanEqExpression, LessThanExpression, MapExpression, MulExpression,
		NotEqExpression, OrExpression, ParameterExpression, PrefixExpression, PrefixOperator, RemExpression,
		SubExpression, TupleExpression, TypeExpression, VariableExpression, XorExpression,
	},
	plan::physical::PhysicalPlan,
};

/// Converts an Expression<'a> to Expression<'static> by converting all
/// fragments to owned
pub fn to_owned_expression(expr: Expression<'_>) -> Expression<'static> {
	match expr {
		Expression::AccessSource(access) => Expression::AccessSource(AccessSourceExpression {
			column: access.column.into_owned(),
		}),
		Expression::Alias(alias) => Expression::Alias(AliasExpression {
			alias: IdentExpression(Fragment::Owned(alias.alias.0.into_owned())),
			expression: Box::new(to_owned_expression(*alias.expression)),
			fragment: Fragment::Owned(alias.fragment.into_owned()),
		}),
		Expression::Cast(cast) => Expression::Cast(CastExpression {
			expression: Box::new(to_owned_expression(*cast.expression)),
			fragment: Fragment::Owned(cast.fragment.into_owned()),
			to: to_owned_type_expression(cast.to),
		}),
		Expression::Constant(constant) => Expression::Constant(to_owned_constant_expression(constant)),
		Expression::Column(column) => Expression::Column(ColumnExpression(column.0.into_owned())),
		Expression::Add(add) => Expression::Add(AddExpression {
			left: Box::new(to_owned_expression(*add.left)),
			right: Box::new(to_owned_expression(*add.right)),
			fragment: Fragment::Owned(add.fragment.into_owned()),
		}),
		Expression::Div(div) => Expression::Div(DivExpression {
			left: Box::new(to_owned_expression(*div.left)),
			right: Box::new(to_owned_expression(*div.right)),
			fragment: Fragment::Owned(div.fragment.into_owned()),
		}),
		Expression::Call(call) => Expression::Call(CallExpression {
			func: IdentExpression(Fragment::Owned(call.func.0.into_owned())),
			args: call.args.into_iter().map(to_owned_expression).collect(),
			fragment: Fragment::Owned(call.fragment.into_owned()),
		}),
		Expression::Rem(rem) => Expression::Rem(RemExpression {
			left: Box::new(to_owned_expression(*rem.left)),
			right: Box::new(to_owned_expression(*rem.right)),
			fragment: Fragment::Owned(rem.fragment.into_owned()),
		}),
		Expression::Mul(mul) => Expression::Mul(MulExpression {
			left: Box::new(to_owned_expression(*mul.left)),
			right: Box::new(to_owned_expression(*mul.right)),
			fragment: Fragment::Owned(mul.fragment.into_owned()),
		}),
		Expression::Sub(sub) => Expression::Sub(SubExpression {
			left: Box::new(to_owned_expression(*sub.left)),
			right: Box::new(to_owned_expression(*sub.right)),
			fragment: Fragment::Owned(sub.fragment.into_owned()),
		}),
		Expression::Tuple(tuple) => Expression::Tuple(TupleExpression {
			expressions: tuple.expressions.into_iter().map(to_owned_expression).collect(),
			fragment: Fragment::Owned(tuple.fragment.into_owned()),
		}),
		Expression::Prefix(prefix) => Expression::Prefix(PrefixExpression {
			operator: to_owned_prefix_operator(prefix.operator),
			expression: Box::new(to_owned_expression(*prefix.expression)),
			fragment: Fragment::Owned(prefix.fragment.into_owned()),
		}),
		Expression::GreaterThan(gt) => Expression::GreaterThan(GreaterThanExpression {
			left: Box::new(to_owned_expression(*gt.left)),
			right: Box::new(to_owned_expression(*gt.right)),
			fragment: Fragment::Owned(gt.fragment.into_owned()),
		}),
		Expression::LessThan(lt) => Expression::LessThan(LessThanExpression {
			left: Box::new(to_owned_expression(*lt.left)),
			right: Box::new(to_owned_expression(*lt.right)),
			fragment: Fragment::Owned(lt.fragment.into_owned()),
		}),
		Expression::GreaterThanEqual(gte) => Expression::GreaterThanEqual(GreaterThanEqExpression {
			left: Box::new(to_owned_expression(*gte.left)),
			right: Box::new(to_owned_expression(*gte.right)),
			fragment: Fragment::Owned(gte.fragment.into_owned()),
		}),
		Expression::LessThanEqual(lte) => Expression::LessThanEqual(LessThanEqExpression {
			left: Box::new(to_owned_expression(*lte.left)),
			right: Box::new(to_owned_expression(*lte.right)),
			fragment: Fragment::Owned(lte.fragment.into_owned()),
		}),
		Expression::Equal(eq) => Expression::Equal(EqExpression {
			left: Box::new(to_owned_expression(*eq.left)),
			right: Box::new(to_owned_expression(*eq.right)),
			fragment: Fragment::Owned(eq.fragment.into_owned()),
		}),
		Expression::NotEqual(ne) => Expression::NotEqual(NotEqExpression {
			left: Box::new(to_owned_expression(*ne.left)),
			right: Box::new(to_owned_expression(*ne.right)),
			fragment: Fragment::Owned(ne.fragment.into_owned()),
		}),
		Expression::And(and) => Expression::And(AndExpression {
			left: Box::new(to_owned_expression(*and.left)),
			right: Box::new(to_owned_expression(*and.right)),
			fragment: Fragment::Owned(and.fragment.into_owned()),
		}),
		Expression::Or(or) => Expression::Or(OrExpression {
			left: Box::new(to_owned_expression(*or.left)),
			right: Box::new(to_owned_expression(*or.right)),
			fragment: Fragment::Owned(or.fragment.into_owned()),
		}),
		Expression::Xor(xor) => Expression::Xor(XorExpression {
			left: Box::new(to_owned_expression(*xor.left)),
			right: Box::new(to_owned_expression(*xor.right)),
			fragment: Fragment::Owned(xor.fragment.into_owned()),
		}),
		Expression::Between(between) => Expression::Between(BetweenExpression {
			value: Box::new(to_owned_expression(*between.value)),
			lower: Box::new(to_owned_expression(*between.lower)),
			upper: Box::new(to_owned_expression(*between.upper)),
			fragment: Fragment::Owned(between.fragment.into_owned()),
		}),
		Expression::Type(type_expr) => Expression::Type(to_owned_type_expression(type_expr)),
		Expression::Parameter(param) => Expression::Parameter(to_owned_parameter_expression(param)),
		Expression::Variable(var) => Expression::Variable(to_owned_variable_expression(var)),
		Expression::If(if_expr) => Expression::If(to_owned_if_expression(if_expr)),
		Expression::Map(map_expr) => Expression::Map(to_owned_map_expression(map_expr)),
		Expression::Extend(extend_expr) => Expression::Extend(to_owned_extend_expression(extend_expr)),
	}
}

fn to_owned_constant_expression(constant: ConstantExpression<'_>) -> ConstantExpression<'static> {
	match constant {
		ConstantExpression::Undefined {
			fragment,
		} => ConstantExpression::Undefined {
			fragment: Fragment::Owned(fragment.into_owned()),
		},
		ConstantExpression::Bool {
			fragment,
		} => ConstantExpression::Bool {
			fragment: Fragment::Owned(fragment.into_owned()),
		},
		ConstantExpression::Number {
			fragment,
		} => ConstantExpression::Number {
			fragment: Fragment::Owned(fragment.into_owned()),
		},
		ConstantExpression::Text {
			fragment,
		} => ConstantExpression::Text {
			fragment: Fragment::Owned(fragment.into_owned()),
		},
		ConstantExpression::Temporal {
			fragment,
		} => ConstantExpression::Temporal {
			fragment: Fragment::Owned(fragment.into_owned()),
		},
	}
}

fn to_owned_type_expression(type_expr: TypeExpression<'_>) -> TypeExpression<'static> {
	TypeExpression {
		ty: type_expr.ty,
		fragment: Fragment::Owned(type_expr.fragment.into_owned()),
	}
}

/// Helper function to convert ParameterExpression<'a> to
/// ParameterExpression<'static>
fn to_owned_parameter_expression(param: ParameterExpression<'_>) -> ParameterExpression<'static> {
	match param {
		ParameterExpression::Positional {
			fragment,
		} => ParameterExpression::Positional {
			fragment: Fragment::Owned(fragment.into_owned()),
		},
		ParameterExpression::Named {
			fragment,
		} => ParameterExpression::Named {
			fragment: Fragment::Owned(fragment.into_owned()),
		},
	}
}

fn to_owned_variable_expression(var: VariableExpression<'_>) -> VariableExpression<'static> {
	VariableExpression {
		fragment: Fragment::Owned(var.fragment.into_owned()),
	}
}

fn to_owned_if_expression(if_expr: IfExpression<'_>) -> IfExpression<'static> {
	IfExpression {
		condition: Box::new(to_owned_expression(*if_expr.condition)),
		then_expr: Box::new(to_owned_expression(*if_expr.then_expr)),
		else_ifs: if_expr
			.else_ifs
			.into_iter()
			.map(|else_if| ElseIfExpression {
				condition: Box::new(to_owned_expression(*else_if.condition)),
				then_expr: Box::new(to_owned_expression(*else_if.then_expr)),
				fragment: Fragment::Owned(else_if.fragment.into_owned()),
			})
			.collect(),
		else_expr: if_expr.else_expr.map(|else_expr| Box::new(to_owned_expression(*else_expr))),
		fragment: Fragment::Owned(if_expr.fragment.into_owned()),
	}
}

fn to_owned_prefix_operator(op: PrefixOperator<'_>) -> PrefixOperator<'static> {
	match op {
		PrefixOperator::Minus(fragment) => PrefixOperator::Minus(Fragment::Owned(fragment.into_owned())),
		PrefixOperator::Plus(fragment) => PrefixOperator::Plus(Fragment::Owned(fragment.into_owned())),
		PrefixOperator::Not(fragment) => PrefixOperator::Not(Fragment::Owned(fragment.into_owned())),
	}
}

/// Converts a vector of expressions to owned forms
pub fn to_owned_expressions(exprs: Vec<Expression<'_>>) -> Vec<Expression<'static>> {
	exprs.into_iter().map(to_owned_expression).collect()
}

pub fn to_owned_fragment(fragment: Fragment<'_>) -> Fragment<'static> {
	Fragment::Owned(fragment.into_owned())
}

pub fn to_owned_physical_plan(plan: PhysicalPlan<'_>) -> PhysicalPlan<'static> {
	match plan {
		PhysicalPlan::Aggregate(node) => PhysicalPlan::Aggregate(crate::plan::physical::AggregateNode {
			input: Box::new(to_owned_physical_plan(*node.input)),
			by: to_owned_expressions(node.by),
			map: to_owned_expressions(node.map),
		}),
		PhysicalPlan::Map(node) => PhysicalPlan::Map(crate::plan::physical::MapNode {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			map: to_owned_expressions(node.map),
		}),
		PhysicalPlan::Filter(node) => PhysicalPlan::Filter(crate::plan::physical::FilterNode {
			input: Box::new(to_owned_physical_plan(*node.input)),
			conditions: to_owned_expressions(node.conditions),
		}),
		PhysicalPlan::Sort(node) => PhysicalPlan::Sort(crate::plan::physical::SortNode {
			input: Box::new(to_owned_physical_plan(*node.input)),
			by: node.by, // SortKey doesn't contain fragments
		}),
		PhysicalPlan::Take(node) => PhysicalPlan::Take(crate::plan::physical::TakeNode {
			input: Box::new(to_owned_physical_plan(*node.input)),
			take: node.take,
		}),
		PhysicalPlan::Distinct(node) => PhysicalPlan::Distinct(crate::plan::physical::DistinctNode {
			input: Box::new(to_owned_physical_plan(*node.input)),
			columns: node.columns.into_iter().map(|c| c.to_static()).collect(),
		}),
		PhysicalPlan::JoinInner(node) => PhysicalPlan::JoinInner(crate::plan::physical::JoinInnerNode {
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			on: to_owned_expressions(node.on),
			alias: node.alias.map(|a| Fragment::Owned(a.into_owned())),
			strategy: node.strategy.clone(),
			right_query: node.right_query.clone(),
		}),
		PhysicalPlan::JoinLeft(node) => PhysicalPlan::JoinLeft(crate::plan::physical::JoinLeftNode {
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			on: to_owned_expressions(node.on),
			alias: node.alias.map(|a| Fragment::Owned(a.into_owned())),
			strategy: node.strategy.clone(),
			right_query: node.right_query.clone(),
		}),
		PhysicalPlan::Extend(node) => PhysicalPlan::Extend(crate::plan::physical::ExtendNode {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			extend: to_owned_expressions(node.extend),
		}),
		PhysicalPlan::Apply(node) => PhysicalPlan::Apply(crate::plan::physical::ApplyNode {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			operator: to_owned_fragment(node.operator),
			expressions: to_owned_expressions(node.expressions),
		}),
		PhysicalPlan::TableScan(node) => {
			// For TableScan, we need to extract the namespace and table defs
			// from the resolved source and convert them to owned versions
			PhysicalPlan::TableScan(crate::plan::physical::TableScanNode {
				source: node.source.to_static(),
			})
		}
		PhysicalPlan::ViewScan(node) => {
			// For ViewScan, convert the resolved view to owned
			PhysicalPlan::ViewScan(crate::plan::physical::ViewScanNode {
				source: node.source.to_static(),
			})
		}
		PhysicalPlan::RingBufferScan(node) => {
			// For RingBufferScan, convert the resolved ring buffer to owned
			PhysicalPlan::RingBufferScan(crate::plan::physical::RingBufferScanNode {
				source: node.source.to_static(),
			})
		}
		PhysicalPlan::TableVirtualScan(node) => {
			// For TableVirtualScan, convert resolved table virtual and context to owned
			PhysicalPlan::TableVirtualScan(crate::plan::physical::TableVirtualScanNode {
				source: node.source.to_static(),
				pushdown_context: node.pushdown_context.map(|ctx| {
					crate::plan::physical::TableVirtualPushdownContext {
						filters: to_owned_expressions(ctx.filters),
						projections: to_owned_expressions(ctx.projections),
						order_by: ctx.order_by.clone(),
						limit: ctx.limit,
					}
				}),
			})
		}
		PhysicalPlan::IndexScan(node) => {
			// For IndexScan, convert the resolved table to owned
			PhysicalPlan::IndexScan(crate::plan::physical::IndexScanNode {
				source: node.source.to_static(),
				index_name: node.index_name.clone(),
			})
		}
		PhysicalPlan::InlineData(node) => PhysicalPlan::InlineData(crate::plan::physical::InlineDataNode {
			rows: node
				.rows
				.into_iter()
				.map(|row| {
					row.into_iter()
						.map(|alias_expr| AliasExpression {
							alias: IdentExpression(Fragment::Owned(
								alias_expr.alias.0.into_owned(),
							)),
							expression: Box::new(to_owned_expression(
								*alias_expr.expression,
							)),
							fragment: Fragment::Owned(alias_expr.fragment.into_owned()),
						})
						.collect()
				})
				.collect(),
		}),
		_ => unimplemented!("Implement conversion for remaining PhysicalPlan variants"),
	}
}

/// Helper function to convert MapExpression<'a> to MapExpression<'static>
fn to_owned_map_expression(map: MapExpression<'_>) -> MapExpression<'static> {
	MapExpression {
		expressions: map.expressions.into_iter().map(to_owned_expression).collect(),
		fragment: Fragment::Owned(map.fragment.into_owned()),
	}
}

/// Helper function to convert ExtendExpression<'a> to ExtendExpression<'static>
fn to_owned_extend_expression(extend: ExtendExpression<'_>) -> ExtendExpression<'static> {
	ExtendExpression {
		expressions: extend.expressions.into_iter().map(to_owned_expression).collect(),
		fragment: Fragment::Owned(extend.fragment.into_owned()),
	}
}
