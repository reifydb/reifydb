// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::JoinType,
	error::diagnostic::operation::{lookup_right_unsupported, lookup_using_not_partition},
	expression::Expression,
	flow::operator::{LookupObject, OperatorDef},
	interface::{
		catalog::{flow::OperatorId, view::View},
		identifier::ColumnObject,
	},
	operator_with::LookupWith,
};
use reifydb_rql::{nodes::LookupNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error, fragment::Fragment};

use crate::compiler::{CompileOperator, FlowCompiler, operator::join::extract_join_keys};

pub(crate) struct LookupCompiler {
	pub join_type: JoinType,
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
	pub with: LookupWith,
}

impl From<LookupNode> for LookupCompiler {
	fn from(node: LookupNode) -> Self {
		Self {
			join_type: node.join_type,
			left: node.left,
			right: node.right,
			on: node.on,
			alias: node.alias,
			with: node.with,
		}
	}
}

struct LookupRight {
	object: LookupObject,
	name: String,
	fragment: Fragment,
	partition_by: Vec<String>,
}

fn lookup_right(plan: &QueryPlan, fallback: &Fragment) -> Result<LookupRight> {
	match plan {
		QueryPlan::TableScan(node) => Ok(LookupRight {
			object: LookupObject::Table(node.source.def().id),
			name: node.source.def().name.clone(),
			fragment: node.source.identifier().clone(),
			partition_by: node.source.def().partition_by.clone(),
		}),
		QueryPlan::ViewScan(node) => match node.source.def() {
			View::Table(view) if view.sort.is_empty() => Ok(LookupRight {
				object: LookupObject::View(view.id),
				name: view.name.clone(),
				fragment: node.source.identifier().clone(),
				partition_by: view.partition_by.clone(),
			}),
			_ => Err(error!(lookup_right_unsupported(
				node.source.identifier().clone(),
				&node.source.fully_qualified_name()
			))),
		},
		QueryPlan::RingBufferScan(node) => Err(error!(lookup_right_unsupported(
			node.source.identifier().clone(),
			&node.source.fully_qualified_name()
		))),
		QueryPlan::SeriesScan(node) => Err(error!(lookup_right_unsupported(
			node.source.identifier().clone(),
			&node.source.fully_qualified_name()
		))),
		QueryPlan::RemoteScan(node) => {
			Err(error!(lookup_right_unsupported(fallback.clone(), &node.remote_name)))
		}
		other => Err(error!(lookup_right_unsupported(fallback.clone(), other.name()))),
	}
}

fn right_key_name(key: &Expression, alias: &str) -> Option<String> {
	let Expression::AccessSource(access) = key else {
		return None;
	};
	match &access.column.object {
		ColumnObject::Alias(object) if object.text() == alias => Some(access.column.name.text().to_string()),
		_ => None,
	}
}

fn order_left_keys(
	left_keys: Vec<Expression>,
	right_keys: &[Expression],
	alias: &str,
	right: &LookupRight,
) -> Result<Vec<Expression>> {
	let not_partition = |fragment: Fragment| error!(lookup_using_not_partition(fragment, &right.name));

	let mut names = Vec::with_capacity(right_keys.len());
	for key in right_keys {
		match right_key_name(key, alias) {
			Some(name) => names.push(name),
			None => return Err(not_partition(key.full_fragment_owned())),
		}
	}

	if names.len() != right.partition_by.len() {
		return Err(not_partition(right.fragment.clone()));
	}

	let mut slots: Vec<Option<Expression>> = vec![None; right.partition_by.len()];
	for (name, (left_key, right_key)) in names.iter().zip(left_keys.into_iter().zip(right_keys)) {
		let Some(position) = right.partition_by.iter().position(|column| column == name) else {
			return Err(not_partition(right_key.full_fragment_owned()));
		};
		if slots[position].is_some() {
			return Err(not_partition(right_key.full_fragment_owned()));
		}
		slots[position] = Some(left_key);
	}

	Ok(slots.into_iter().flatten().collect())
}

impl CompileOperator for LookupCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let alias_fragment = self.alias.clone().unwrap_or(Fragment::None);
		let right = lookup_right(&self.right, &alias_fragment)?;
		let alias = self.alias.as_ref().map(|f| f.text().to_string()).unwrap_or_else(|| right.name.clone());

		let (left_keys, right_keys) = extract_join_keys(&self.on);
		let left = order_left_keys(left_keys, &right_keys, &alias, &right)?;

		let left_node = compiler.compile_plan(txn, *self.left)?;

		let node_id = compiler.add_node(
			txn,
			OperatorDef::Lookup {
				join_type: self.join_type,
				right: right.object,
				left,
				alias: Some(alias),
				with: self.with.clone(),
			},
		)?;

		compiler.write_operator_retention(
			txn,
			node_id,
			self.with.retention.as_ref().map(|retention| retention.duration),
		)?;

		compiler.add_edge(txn, &left_node, &node_id)?;

		Ok(node_id)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		expression::{AccessObjectExpression, AndExpression, ColumnExpression, EqExpression},
		interface::{catalog::id::TableId, identifier::ColumnIdentifier},
	};

	use super::*;

	fn left_column(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			object: ColumnObject::Alias(Fragment::internal("l")),
			name: Fragment::internal(name),
		}))
	}

	fn right_column(alias: &str, name: &str) -> Expression {
		Expression::AccessSource(AccessObjectExpression {
			column: ColumnIdentifier {
				object: ColumnObject::Alias(Fragment::internal(alias)),
				name: Fragment::internal(name),
			},
		})
	}

	fn using(pairs: &[(Expression, Expression)]) -> Vec<Expression> {
		let equals = pairs.iter().map(|(left, right)| {
			Expression::Equal(EqExpression {
				left: Box::new(left.clone()),
				right: Box::new(right.clone()),
				fragment: Fragment::None,
			})
		});
		let combined = equals
			.reduce(|acc, expr| {
				Expression::And(AndExpression {
					left: Box::new(acc),
					right: Box::new(expr),
					fragment: Fragment::None,
				})
			})
			.expect("a using clause has at least one pair");
		vec![combined]
	}

	fn pool(partition_by: &[&str]) -> LookupRight {
		LookupRight {
			object: LookupObject::Table(TableId(1)),
			name: "pool".to_string(),
			fragment: Fragment::None,
			partition_by: partition_by.iter().map(|column| column.to_string()).collect(),
		}
	}

	fn order(on: &[Expression], right: &LookupRight) -> Result<Vec<Expression>> {
		let (left_keys, right_keys) = extract_join_keys(on);
		order_left_keys(left_keys, &right_keys, "info", right)
	}

	fn code(result: Result<Vec<Expression>>) -> String {
		result.expect_err("the shape must be rejected").diagnostic().code
	}

	#[test]
	fn using_pairs_written_out_of_partition_order_are_reordered_to_it() {
		// G1: the hash takes values in partition order, so keeping the written order would hash (pair, pool).
		let on = using(&[
			(left_column("pair_l"), right_column("info", "pair")),
			(left_column("pool_l"), right_column("info", "pool")),
		]);

		let left = order(&on, &pool(&["pool", "pair"])).expect("the using columns are the partition columns");

		assert_eq!(left, vec![left_column("pool_l"), left_column("pair_l")]);
	}

	#[test]
	fn a_using_clause_missing_a_partition_column_is_rejected_with_lookup_002() {
		// MD13: a partial key would have to scan several partitions, which the lookup never does.
		let on = using(&[(left_column("pool_l"), right_column("info", "pool"))]);

		assert_eq!(code(order(&on, &pool(&["pool", "pair"]))), "LOOKUP_002");
	}

	#[test]
	fn a_using_column_outside_the_partition_is_rejected_with_lookup_002() {
		// An extra equality cannot be answered by one partition read, so it must not be silently dropped.
		let on = using(&[
			(left_column("pool_l"), right_column("info", "pool")),
			(left_column("pair_l"), right_column("info", "pair")),
		]);

		assert_eq!(code(order(&on, &pool(&["pool"]))), "LOOKUP_002");
	}

	#[test]
	fn a_partition_column_named_twice_is_rejected_with_lookup_002() {
		// Same count as the partition, but one column is covered twice and the other not at all.
		let on = using(&[
			(left_column("pool_l"), right_column("info", "pool")),
			(left_column("other_l"), right_column("info", "pool")),
		]);

		assert_eq!(code(order(&on, &pool(&["pool", "pair"]))), "LOOKUP_002");
	}

	#[test]
	fn a_right_key_not_qualified_by_the_alias_is_rejected_with_lookup_002() {
		// Without the alias the second column is not known to name a right column, so no partition can match.
		let on = using(&[(left_column("pool_l"), left_column("pool"))]);

		assert_eq!(code(order(&on, &pool(&["pool"]))), "LOOKUP_002");
	}

	#[test]
	fn a_right_key_qualified_by_another_alias_is_rejected_with_lookup_002() {
		// Only the lookup's own alias names the right side; any other qualifier points elsewhere.
		let on = using(&[(left_column("pool_l"), right_column("base", "pool"))]);

		assert_eq!(code(order(&on, &pool(&["pool"]))), "LOOKUP_002");
	}

	#[test]
	fn a_right_object_with_no_partition_is_rejected_with_lookup_002() {
		// An unpartitioned object has no partition to read, so no using clause can equal its partition columns.
		let on = using(&[(left_column("pool_l"), right_column("info", "pool"))]);

		assert_eq!(code(order(&on, &pool(&[]))), "LOOKUP_002");
	}
}
