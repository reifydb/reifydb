// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use bumpalo::Bump;
use datafusion_common::tree_node::TreeNode;
use reifydb_core::expression::{
	ConstantExpression, Expression,
	name::{column_refs, display_label},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_value::value::{Value, system_columns::SystemColumn};

use crate::{
	bump::BumpBox,
	plan::logical::{ExtendNode, FilterNode, MapNode, tree::QueryTree},
};

pub(crate) fn optimize_tree<'bump>(bump: &'bump Bump, routines: &Routines, tree: QueryTree<'bump>) -> QueryTree<'bump> {
	let rules = Rules {
		bump,
		routines,
	};
	rules.optimize(tree)
}

struct Rules<'a, 'bump> {
	bump: &'bump Bump,
	routines: &'a Routines,
}

type Input<'bump> = BumpBox<'bump, QueryTree<'bump>>;

impl<'a, 'bump> Rules<'a, 'bump> {
	fn optimize(&self, tree: QueryTree<'bump>) -> QueryTree<'bump> {
		match self.map_inputs(tree) {
			QueryTree::Filter {
				node,
				input,
			} if is_true(&node.condition) => BumpBox::into_inner(input),
			QueryTree::Filter {
				node,
				input,
			} => self.push_filter(node, input),
			other => other,
		}
	}

	fn push_filter(&self, filter: FilterNode, input: Input<'bump>) -> QueryTree<'bump> {
		if self.blocks_moving(&filter) {
			return QueryTree::Filter {
				node: filter,
				input,
			};
		}
		match BumpBox::into_inner(input) {
			QueryTree::Map {
				node,
				input: Some(inner),
			} if self.passes_map(&filter, &node) => QueryTree::Map {
				node,
				input: Some(self.boxed(self.push_filter(filter, inner))),
			},
			QueryTree::Extend {
				node,
				input: Some(inner),
			} if self.passes_extend(&filter, &node) => QueryTree::Extend {
				node,
				input: Some(self.boxed(self.push_filter(filter, inner))),
			},
			QueryTree::Order {
				node,
				input: inner,
			} => QueryTree::Order {
				node,
				input: self.boxed(self.push_filter(filter, inner)),
			},
			other => QueryTree::Filter {
				node: filter,
				input: self.boxed(other),
			},
		}
	}

	fn blocks_moving(&self, filter: &FilterNode) -> bool {
		let qualified = filter
			.condition
			.exists(|e| Ok(matches!(e, Expression::AccessSource(_))))
			.expect("traversal is infallible");
		qualified || self.changes_row_count(&filter.condition)
	}

	fn passes_map(&self, filter: &FilterNode, map: &MapNode) -> bool {
		if map.map.iter().any(|e| self.changes_row_count(e)) {
			return false;
		}
		column_refs(&filter.condition).iter().all(|r| {
			let name = r.name.text();
			is_system_column(name)
				|| map.map.iter().any(|e| matches!(e, Expression::Column(c) if c.0.name.text() == name))
		})
	}

	fn passes_extend(&self, filter: &FilterNode, extend: &ExtendNode) -> bool {
		if extend.extend.iter().any(|e| self.changes_row_count(e)) {
			return false;
		}
		let written: Vec<_> = extend.extend.iter().map(display_label).collect();
		column_refs(&filter.condition).iter().all(|r| written.iter().all(|w| w.text() != r.name.text()))
	}

	fn changes_row_count(&self, expr: &Expression) -> bool {
		expr.exists(|e| {
			Ok(matches!(e, Expression::Call(call) if self
				.routines
				.get_scalar_function(call.func.0.text())
				.is_some_and(|function| function.changes_row_count())))
		})
		.expect("traversal is infallible")
	}

	fn boxed(&self, tree: QueryTree<'bump>) -> Input<'bump> {
		BumpBox::new_in(tree, self.bump)
	}

	fn opt(&self, input: Input<'bump>) -> Input<'bump> {
		self.boxed(self.optimize(BumpBox::into_inner(input)))
	}

	fn opt_some(&self, input: Option<Input<'bump>>) -> Option<Input<'bump>> {
		input.map(|input| self.opt(input))
	}

	fn map_inputs(&self, tree: QueryTree<'bump>) -> QueryTree<'bump> {
		match tree {
			QueryTree::Aggregate {
				node,
				input,
			} => QueryTree::Aggregate {
				node,
				input: self.opt(input),
			},
			QueryTree::Filter {
				node,
				input,
			} => QueryTree::Filter {
				node,
				input: self.opt(input),
			},
			QueryTree::Gate {
				node,
				input,
			} => QueryTree::Gate {
				node,
				input: self.opt(input),
			},
			QueryTree::Order {
				node,
				input,
			} => QueryTree::Order {
				node,
				input: self.opt(input),
			},
			QueryTree::Distinct {
				node,
				input,
			} => QueryTree::Distinct {
				node,
				input: self.opt(input),
			},
			QueryTree::Take {
				node,
				input,
			} => QueryTree::Take {
				node,
				input: self.opt(input),
			},
			QueryTree::Assert {
				node,
				input,
			} => QueryTree::Assert {
				node,
				input: self.opt_some(input),
			},
			QueryTree::Map {
				node,
				input,
			} => QueryTree::Map {
				node,
				input: self.opt_some(input),
			},
			QueryTree::Extend {
				node,
				input,
			} => QueryTree::Extend {
				node,
				input: self.opt_some(input),
			},
			QueryTree::Patch {
				node,
				input,
			} => QueryTree::Patch {
				node,
				input: self.opt_some(input),
			},
			QueryTree::Apply {
				node,
				input,
			} => QueryTree::Apply {
				node,
				input: self.opt_some(input),
			},
			QueryTree::Window {
				node,
				input,
			} => QueryTree::Window {
				node,
				input: self.opt_some(input),
			},
			QueryTree::JoinInner {
				on,
				alias,
				with,
				rql,
				input,
				right,
			} => QueryTree::JoinInner {
				on,
				alias,
				with,
				rql,
				input: self.opt(input),
				right: self.opt(right),
			},
			QueryTree::JoinLeft {
				on,
				alias,
				with,
				rql,
				input,
				right,
			} => QueryTree::JoinLeft {
				on,
				alias,
				with,
				rql,
				input: self.opt(input),
				right: self.opt(right),
			},
			QueryTree::JoinNatural {
				join_type,
				fragment,
				alias,
				with,
				rql,
				input,
				right,
			} => QueryTree::JoinNatural {
				join_type,
				fragment,
				alias,
				with,
				rql,
				input: self.opt(input),
				right: self.opt(right),
			},
			QueryTree::Lookup {
				join_type,
				on,
				alias,
				with,
				input,
				right,
			} => QueryTree::Lookup {
				join_type,
				on,
				alias,
				with,
				input: self.opt(input),
				right: self.opt(right),
			},
			QueryTree::AppendQuery {
				fragment,
				input,
				right,
			} => QueryTree::AppendQuery {
				fragment,
				input: self.opt(input),
				right: self.opt(right),
			},
			QueryTree::DeleteTable {
				target,
				returning,
				input,
			} => QueryTree::DeleteTable {
				target,
				returning,
				input: self.opt_some(input),
			},
			QueryTree::DeleteRingBuffer {
				target,
				returning,
				input,
			} => QueryTree::DeleteRingBuffer {
				target,
				returning,
				input: self.opt_some(input),
			},
			QueryTree::DeleteSeries {
				target,
				returning,
				input,
			} => QueryTree::DeleteSeries {
				target,
				returning,
				input: self.opt_some(input),
			},
			QueryTree::Update {
				target,
				returning,
				input,
			} => QueryTree::Update {
				target,
				returning,
				input: self.opt(input),
			},
			QueryTree::UpdateRingBuffer {
				target,
				returning,
				input,
			} => QueryTree::UpdateRingBuffer {
				target,
				returning,
				input: self.opt(input),
			},
			QueryTree::UpdateSeries {
				target,
				returning,
				input,
			} => QueryTree::UpdateSeries {
				target,
				returning,
				input: self.opt(input),
			},
			leaf @ (QueryTree::SourceScan {
				..
			}
			| QueryTree::RemoteScan {
				..
			}
			| QueryTree::InlineData {
				..
			}
			| QueryTree::Generator {
				..
			}
			| QueryTree::VariableSource {
				..
			}
			| QueryTree::Environment {
				..
			}
			| QueryTree::Statement(_)) => leaf,
		}
	}
}

fn is_true(condition: &Expression) -> bool {
	matches!(condition, Expression::Constant(constant @ ConstantExpression::Bool { .. }) if matches!(constant.to_value(), Ok(Value::Boolean(true))))
}

fn is_system_column(name: &str) -> bool {
	let bare = name.strip_prefix('#').unwrap_or(name);
	SystemColumn::ALL.iter().any(|c| &c.name()[1..] == bare)
}
