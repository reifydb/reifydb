// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::resolved::ResolvedShape;
use reifydb_rql::{
	expression::{AliasExpression, ConstantExpression, Expression, IdentExpression},
	nodes::{
		ExtendNode as RqlExtendNode, FilterNode as RqlFilterNode, MapNode as RqlMapNode,
		PatchNode as RqlPatchNode,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::constraint::Constraint};

use super::{compile, extract_resolved_source};
use crate::vm::volcano::{
	extend::{ExtendNode, ExtendWithoutInputNode},
	filter::{FilterNode, resolve_is_variant_tags},
	map::{MapNode, MapWithoutInputNode},
	patch::PatchNode,
	query::{QueryContext, QueryNode},
};

pub(crate) fn compile_filter<'a>(
	node: RqlFilterNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	let mut conditions = node.conditions;
	if let Some(source) = extract_resolved_source(&node.input) {
		for expr in &mut conditions {
			resolve_is_variant_tags(expr, &source, &context.services.catalog, rx)
				.expect("resolve IS variant tags");
		}
	}
	let input_node = compile(*node.input, rx, context);
	Box::new(FilterNode::new(input_node, conditions))
}

pub(crate) fn compile_map<'a>(
	node: RqlMapNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	let mut map = node.map;
	if let Some(input) = node.input {
		if let Some(source) = extract_resolved_source(&input) {
			for expr in &mut map {
				resolve_is_variant_tags(expr, &source, &context.services.catalog, rx)
					.expect("resolve IS variant tags in map");
			}
		}
		let input_node = compile(*input, rx, context);
		Box::new(MapNode::new(input_node, map))
	} else {
		Box::new(MapWithoutInputNode::new(map))
	}
}

pub(crate) fn compile_extend<'a>(
	node: RqlExtendNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	let mut extend = node.extend;
	if let Some(input) = node.input {
		if let Some(source) = extract_resolved_source(&input) {
			for expr in &mut extend {
				resolve_is_variant_tags(expr, &source, &context.services.catalog, rx)
					.expect("resolve IS variant tags in extend");
			}
		}
		let input_node = compile(*input, rx, context);
		Box::new(ExtendNode::new(input_node, extend))
	} else {
		Box::new(ExtendWithoutInputNode::new(extend))
	}
}

pub(crate) fn compile_patch<'a>(
	node: RqlPatchNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	let mut assignments = node.assignments;
	let input = node.input.expect("Patch requires input");

	if let Some(source) = extract_resolved_source(&input) {
		assignments = expand_patch_sumtype_assignments(assignments, &source, &context.services.catalog, rx);
	}

	let input_node = compile(*input, rx, context);
	Box::new(PatchNode::new(input_node, assignments))
}

fn expand_patch_sumtype_assignments(
	assignments: Vec<Expression>,
	source: &ResolvedShape,
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
) -> Vec<Expression> {
	let mut expanded = Vec::with_capacity(assignments.len());

	for expr in assignments {
		let Expression::Alias(ref alias_expr) = expr else {
			expanded.push(expr);
			continue;
		};

		let col_name = alias_expr.alias.name().to_string();
		let tag_col_name = format!("{}_tag", col_name);

		let tag_col = source.columns().iter().find(|c| c.name == tag_col_name);
		let sumtype_info = tag_col.and_then(|tc| {
			if let Some(Constraint::SumType(id)) = tc.constraint.constraint() {
				catalog.get_sumtype(rx, *id).ok().map(|def| (def, *id))
			} else {
				None
			}
		});

		let Some((sumtype, _)) = sumtype_info else {
			expanded.push(expr);
			continue;
		};

		let fragment = alias_expr.fragment.clone();

		match alias_expr.expression.as_ref() {
			Expression::SumTypeConstructor(ctor) => {
				let variant_name_lower = ctor.variant_name.text().to_lowercase();
				let variant = sumtype
					.variants
					.iter()
					.find(|v| v.name.to_lowercase() == variant_name_lower)
					.expect("variant not found in sumtype");

				expanded.push(Expression::Alias(AliasExpression {
					alias: IdentExpression(Fragment::internal(format!("{}_tag", col_name))),
					expression: Box::new(Expression::Constant(ConstantExpression::Number {
						fragment: Fragment::internal(variant.tag.to_string()),
					})),
					fragment: fragment.clone(),
				}));

				let field_map: collections::HashMap<String, &Expression> = ctor
					.columns
					.iter()
					.map(|(name, expr)| (name.text().to_lowercase(), expr))
					.collect();

				for v in &sumtype.variants {
					for field in &v.fields {
						let phys_col_name = format!(
							"{}_{}_{}",
							col_name,
							v.name.to_lowercase(),
							field.name.to_lowercase()
						);
						let field_expr = if v.name.to_lowercase() == variant_name_lower {
							if let Some(e) = field_map.get(&field.name.to_lowercase()) {
								(*e).clone()
							} else {
								Expression::Constant(ConstantExpression::None {
									fragment: fragment.clone(),
								})
							}
						} else {
							Expression::Constant(ConstantExpression::None {
								fragment: fragment.clone(),
							})
						};
						expanded.push(Expression::Alias(AliasExpression {
							alias: IdentExpression(Fragment::internal(phys_col_name)),
							expression: Box::new(field_expr),
							fragment: fragment.clone(),
						}));
					}
				}
			}
			Expression::Column(col) => {
				let variant_name_lower = col.0.name.text().to_lowercase();
				if let Some(variant) =
					sumtype.variants.iter().find(|v| v.name.to_lowercase() == variant_name_lower)
				{
					expanded.push(Expression::Alias(AliasExpression {
						alias: IdentExpression(Fragment::internal(format!("{}_tag", col_name))),
						expression: Box::new(Expression::Constant(
							ConstantExpression::Number {
								fragment: Fragment::internal(variant.tag.to_string()),
							},
						)),
						fragment: fragment.clone(),
					}));

					for v in &sumtype.variants {
						for field in &v.fields {
							let phys_col_name = format!(
								"{}_{}_{}",
								col_name,
								v.name.to_lowercase(),
								field.name.to_lowercase()
							);
							expanded.push(Expression::Alias(AliasExpression {
								alias: IdentExpression(Fragment::internal(
									phys_col_name,
								)),
								expression: Box::new(Expression::Constant(
									ConstantExpression::None {
										fragment: fragment.clone(),
									},
								)),
								fragment: fragment.clone(),
							}));
						}
					}
				} else {
					expanded.push(expr);
				}
			}
			_ => {
				expanded.push(expr);
			}
		}
	}

	expanded
}
