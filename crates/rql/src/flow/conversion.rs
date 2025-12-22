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
		IdentExpression, IfExpression, InExpression, LessThanEqExpression, LessThanExpression, MapExpression,
		MulExpression, NotEqExpression, OrExpression, ParameterExpression, PrefixExpression, PrefixOperator,
		RemExpression, SubExpression, TupleExpression, TypeExpression, VariableExpression, XorExpression,
	},
	plan::physical::PhysicalPlan,
};

/// Converts an Expression to Expression by converting all
/// fragments to owned
pub fn to_owned_expression(expr: Expression) -> Expression {
	match expr {
		Expression::AccessSource(access) => Expression::AccessSource(AccessSourceExpression {
			column: access.column,
		}),
		Expression::Alias(alias) => Expression::Alias(AliasExpression {
			alias: IdentExpression(alias.alias.0),
			expression: Box::new(to_owned_expression(*alias.expression)),
			fragment: alias.fragment,
		}),
		Expression::Cast(cast) => Expression::Cast(CastExpression {
			expression: Box::new(to_owned_expression(*cast.expression)),
			fragment: cast.fragment,
			to: to_owned_type_expression(cast.to),
		}),
		Expression::Constant(constant) => Expression::Constant(to_owned_constant_expression(constant)),
		Expression::Column(column) => Expression::Column(ColumnExpression(column.0)),
		Expression::Add(add) => Expression::Add(AddExpression {
			left: Box::new(to_owned_expression(*add.left)),
			right: Box::new(to_owned_expression(*add.right)),
			fragment: add.fragment,
		}),
		Expression::Div(div) => Expression::Div(DivExpression {
			left: Box::new(to_owned_expression(*div.left)),
			right: Box::new(to_owned_expression(*div.right)),
			fragment: div.fragment,
		}),
		Expression::Call(call) => Expression::Call(CallExpression {
			func: IdentExpression(call.func.0),
			args: call.args.into_iter().map(to_owned_expression).collect(),
			fragment: call.fragment,
		}),
		Expression::Rem(rem) => Expression::Rem(RemExpression {
			left: Box::new(to_owned_expression(*rem.left)),
			right: Box::new(to_owned_expression(*rem.right)),
			fragment: rem.fragment,
		}),
		Expression::Mul(mul) => Expression::Mul(MulExpression {
			left: Box::new(to_owned_expression(*mul.left)),
			right: Box::new(to_owned_expression(*mul.right)),
			fragment: mul.fragment,
		}),
		Expression::Sub(sub) => Expression::Sub(SubExpression {
			left: Box::new(to_owned_expression(*sub.left)),
			right: Box::new(to_owned_expression(*sub.right)),
			fragment: sub.fragment,
		}),
		Expression::Tuple(tuple) => Expression::Tuple(TupleExpression {
			expressions: tuple.expressions.into_iter().map(to_owned_expression).collect(),
			fragment: tuple.fragment,
		}),
		Expression::Prefix(prefix) => Expression::Prefix(PrefixExpression {
			operator: to_owned_prefix_operator(prefix.operator),
			expression: Box::new(to_owned_expression(*prefix.expression)),
			fragment: prefix.fragment,
		}),
		Expression::GreaterThan(gt) => Expression::GreaterThan(GreaterThanExpression {
			left: Box::new(to_owned_expression(*gt.left)),
			right: Box::new(to_owned_expression(*gt.right)),
			fragment: gt.fragment,
		}),
		Expression::LessThan(lt) => Expression::LessThan(LessThanExpression {
			left: Box::new(to_owned_expression(*lt.left)),
			right: Box::new(to_owned_expression(*lt.right)),
			fragment: lt.fragment,
		}),
		Expression::GreaterThanEqual(gte) => Expression::GreaterThanEqual(GreaterThanEqExpression {
			left: Box::new(to_owned_expression(*gte.left)),
			right: Box::new(to_owned_expression(*gte.right)),
			fragment: gte.fragment,
		}),
		Expression::LessThanEqual(lte) => Expression::LessThanEqual(LessThanEqExpression {
			left: Box::new(to_owned_expression(*lte.left)),
			right: Box::new(to_owned_expression(*lte.right)),
			fragment: lte.fragment,
		}),
		Expression::Equal(eq) => Expression::Equal(EqExpression {
			left: Box::new(to_owned_expression(*eq.left)),
			right: Box::new(to_owned_expression(*eq.right)),
			fragment: eq.fragment,
		}),
		Expression::NotEqual(ne) => Expression::NotEqual(NotEqExpression {
			left: Box::new(to_owned_expression(*ne.left)),
			right: Box::new(to_owned_expression(*ne.right)),
			fragment: ne.fragment,
		}),
		Expression::And(and) => Expression::And(AndExpression {
			left: Box::new(to_owned_expression(*and.left)),
			right: Box::new(to_owned_expression(*and.right)),
			fragment: and.fragment,
		}),
		Expression::Or(or) => Expression::Or(OrExpression {
			left: Box::new(to_owned_expression(*or.left)),
			right: Box::new(to_owned_expression(*or.right)),
			fragment: or.fragment,
		}),
		Expression::Xor(xor) => Expression::Xor(XorExpression {
			left: Box::new(to_owned_expression(*xor.left)),
			right: Box::new(to_owned_expression(*xor.right)),
			fragment: xor.fragment,
		}),
		Expression::Between(between) => Expression::Between(BetweenExpression {
			value: Box::new(to_owned_expression(*between.value)),
			lower: Box::new(to_owned_expression(*between.lower)),
			upper: Box::new(to_owned_expression(*between.upper)),
			fragment: between.fragment,
		}),
		Expression::Type(type_expr) => Expression::Type(to_owned_type_expression(type_expr)),
		Expression::Parameter(param) => Expression::Parameter(to_owned_parameter_expression(param)),
		Expression::Variable(var) => Expression::Variable(to_owned_variable_expression(var)),
		Expression::If(if_expr) => Expression::If(to_owned_if_expression(if_expr)),
		Expression::Map(map_expr) => Expression::Map(to_owned_map_expression(map_expr)),
		Expression::Extend(extend_expr) => Expression::Extend(to_owned_extend_expression(extend_expr)),
		Expression::In(in_expr) => Expression::In(InExpression {
			value: Box::new(to_owned_expression(*in_expr.value)),
			list: Box::new(to_owned_expression(*in_expr.list)),
			negated: in_expr.negated,
			fragment: in_expr.fragment,
		}),
	}
}

fn to_owned_constant_expression(constant: ConstantExpression) -> ConstantExpression {
	match constant {
		ConstantExpression::Undefined {
			fragment,
		} => ConstantExpression::Undefined {
			fragment,
		},
		ConstantExpression::Bool {
			fragment,
		} => ConstantExpression::Bool {
			fragment,
		},
		ConstantExpression::Number {
			fragment,
		} => ConstantExpression::Number {
			fragment,
		},
		ConstantExpression::Text {
			fragment,
		} => ConstantExpression::Text {
			fragment,
		},
		ConstantExpression::Temporal {
			fragment,
		} => ConstantExpression::Temporal {
			fragment,
		},
	}
}

fn to_owned_type_expression(type_expr: TypeExpression) -> TypeExpression {
	TypeExpression {
		ty: type_expr.ty,
		fragment: type_expr.fragment,
	}
}

/// Helper function to convert ParameterExpression to
/// ParameterExpression
fn to_owned_parameter_expression(param: ParameterExpression) -> ParameterExpression {
	match param {
		ParameterExpression::Positional {
			fragment,
		} => ParameterExpression::Positional {
			fragment,
		},
		ParameterExpression::Named {
			fragment,
		} => ParameterExpression::Named {
			fragment,
		},
	}
}

fn to_owned_variable_expression(var: VariableExpression) -> VariableExpression {
	VariableExpression {
		fragment: var.fragment,
	}
}

fn to_owned_if_expression(if_expr: IfExpression) -> IfExpression {
	IfExpression {
		condition: Box::new(to_owned_expression(*if_expr.condition)),
		then_expr: Box::new(to_owned_expression(*if_expr.then_expr)),
		else_ifs: if_expr
			.else_ifs
			.into_iter()
			.map(|else_if| ElseIfExpression {
				condition: Box::new(to_owned_expression(*else_if.condition)),
				then_expr: Box::new(to_owned_expression(*else_if.then_expr)),
				fragment: else_if.fragment,
			})
			.collect(),
		else_expr: if_expr.else_expr.map(|else_expr| Box::new(to_owned_expression(*else_expr))),
		fragment: if_expr.fragment,
	}
}

fn to_owned_prefix_operator(op: PrefixOperator) -> PrefixOperator {
	match op {
		PrefixOperator::Minus(fragment) => PrefixOperator::Minus(fragment),
		PrefixOperator::Plus(fragment) => PrefixOperator::Plus(fragment),
		PrefixOperator::Not(fragment) => PrefixOperator::Not(fragment),
	}
}

/// Converts a vector of expressions to owned forms
pub fn to_owned_expressions(exprs: Vec<Expression>) -> Vec<Expression> {
	exprs.into_iter().map(to_owned_expression).collect()
}

pub fn to_owned_fragment(fragment: Fragment) -> Fragment {
	fragment
}

pub fn to_owned_physical_plan(plan: PhysicalPlan) -> PhysicalPlan {
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
			columns: node.columns.into_iter().map(|c| c).collect(),
		}),
		PhysicalPlan::JoinInner(node) => PhysicalPlan::JoinInner(crate::plan::physical::JoinInnerNode {
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			on: to_owned_expressions(node.on),
			alias: node.alias.map(|a| a),
		}),
		PhysicalPlan::JoinLeft(node) => PhysicalPlan::JoinLeft(crate::plan::physical::JoinLeftNode {
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			on: to_owned_expressions(node.on),
			alias: node.alias.map(|a| a),
		}),
		PhysicalPlan::Merge(node) => PhysicalPlan::Merge(crate::plan::physical::MergeNode {
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
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
				source: node.source,
			})
		}
		PhysicalPlan::ViewScan(node) => {
			// For ViewScan, convert the resolved view to owned
			PhysicalPlan::ViewScan(crate::plan::physical::ViewScanNode {
				source: node.source,
			})
		}
		PhysicalPlan::RingBufferScan(node) => {
			// For RingBufferScan, convert the resolved ring buffer to owned
			PhysicalPlan::RingBufferScan(crate::plan::physical::RingBufferScanNode {
				source: node.source,
			})
		}
		PhysicalPlan::TableVirtualScan(node) => {
			// For TableVirtualScan, convert resolved table virtual and context to owned
			PhysicalPlan::TableVirtualScan(crate::plan::physical::TableVirtualScanNode {
				source: node.source,
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
				source: node.source,
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
							alias: IdentExpression(alias_expr.alias.0),
							expression: Box::new(to_owned_expression(
								*alias_expr.expression,
							)),
							fragment: alias_expr.fragment,
						})
						.collect()
				})
				.collect(),
		}),
		PhysicalPlan::Window(node) => PhysicalPlan::Window(crate::plan::physical::WindowNode {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			window_type: node.window_type,
			size: node.size,
			slide: node.slide,
			group_by: to_owned_expressions(node.group_by),
			aggregations: to_owned_expressions(node.aggregations),
			min_events: node.min_events,
			max_window_count: node.max_window_count,
			max_window_age: node.max_window_age,
		}),
		PhysicalPlan::FlowScan(node) => {
			// For FlowScan, convert the resolved flow to owned
			PhysicalPlan::FlowScan(crate::plan::physical::FlowScanNode {
				source: node.source,
			})
		}
		PhysicalPlan::RowPointLookup(node) => {
			PhysicalPlan::RowPointLookup(crate::plan::physical::RowPointLookupNode {
				source: node.source,
				row_number: node.row_number,
			})
		}
		PhysicalPlan::RowListLookup(node) => {
			PhysicalPlan::RowListLookup(crate::plan::physical::RowListLookupNode {
				source: node.source,
				row_numbers: node.row_numbers,
			})
		}
		PhysicalPlan::RowRangeScan(node) => {
			PhysicalPlan::RowRangeScan(crate::plan::physical::RowRangeScanNode {
				source: node.source,
				start: node.start,
				end: node.end,
			})
		}
		_ => unimplemented!("Implement conversion for remaining PhysicalPlan variants"),
	}
}

/// Helper function to convert MapExpression to MapExpression
fn to_owned_map_expression(map: MapExpression) -> MapExpression {
	MapExpression {
		expressions: map.expressions.into_iter().map(to_owned_expression).collect(),
		fragment: map.fragment,
	}
}

/// Helper function to convert ExtendExpression to ExtendExpression
fn to_owned_extend_expression(extend: ExtendExpression) -> ExtendExpression {
	ExtendExpression {
		expressions: extend.expressions.into_iter().map(to_owned_expression).collect(),
		fragment: extend.fragment,
	}
}
