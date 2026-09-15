// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::JoinType::{self, Inner, Left},
	error::diagnostic::operation::{join_pick_column_not_found, natural_join_no_shared_column},
	interface::catalog::flow::OperatorId,
	row::{JoinPick, JoinRetention},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result, error,
	fragment::Fragment,
	value::{
		datetime::{CREATED_AT_COLUMN_NAME, TIME_COLUMN_NAME, UPDATED_AT_COLUMN_NAME},
		row_number::ROW_NUMBER_COLUMN_NAME,
	},
};

use crate::{
	expression::{Expression, name::display_label},
	flow::{
		compiler::{CompileOperator, FlowCompiler},
		operator::OperatorDef,
	},
	nodes::{JoinInnerNode, JoinLeftNode, JoinNaturalNode},
	query::QueryPlan,
};

pub(crate) struct JoinCompiler {
	pub join_type: JoinType,
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<String>,
	pub retention: Option<JoinRetention>,
	pub snapshot: bool,
	pub natural: bool,
	pub fragment: Fragment,
	pub pick: Option<JoinPick>,
}

impl From<JoinInnerNode> for JoinCompiler {
	fn from(node: JoinInnerNode) -> Self {
		Self {
			join_type: Inner,
			left: node.left,
			right: node.right,
			on: node.on,
			alias: node.alias.map(|f| f.text().to_string()),
			retention: node.retention,
			snapshot: node.snapshot,
			natural: false,
			fragment: Fragment::None,
			pick: node.pick,
		}
	}
}

impl From<JoinLeftNode> for JoinCompiler {
	fn from(node: JoinLeftNode) -> Self {
		Self {
			join_type: Left,
			left: node.left,
			right: node.right,
			on: node.on,
			alias: node.alias.map(|f| f.text().to_string()),
			retention: node.retention,
			snapshot: node.snapshot,
			natural: false,
			fragment: Fragment::None,
			pick: node.pick,
		}
	}
}

impl From<JoinNaturalNode> for JoinCompiler {
	fn from(node: JoinNaturalNode) -> Self {
		Self {
			join_type: node.join_type,
			left: node.left,
			right: node.right,
			on: Vec::new(),
			alias: node.alias.map(|f| f.text().to_string()),
			retention: node.retention,
			snapshot: node.snapshot,
			natural: true,
			fragment: node.fragment,
			pick: node.pick,
		}
	}
}

fn extract_source_name(plan: &QueryPlan) -> Option<String> {
	match plan {
		QueryPlan::TableScan(node) => Some(node.source.def().name.clone()),
		QueryPlan::ViewScan(node) => Some(node.source.def().name().to_string()),
		QueryPlan::RingBufferScan(node) => Some(node.source.def().name.clone()),
		QueryPlan::DictionaryScan(node) => Some(node.source.def().name.clone()),

		QueryPlan::Filter(node) => extract_source_name(&node.input),
		QueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name(p)),
		QueryPlan::Take(node) => extract_source_name(&node.input),
		_ => None,
	}
}

fn output_column_names(plan: &QueryPlan) -> Option<Vec<String>> {
	match plan {
		QueryPlan::TableScan(node) => {
			Some(node.source.def().columns.iter().map(|col| col.name.clone()).collect())
		}
		QueryPlan::ViewScan(node) => {
			Some(node.source.def().columns().iter().map(|col| col.name.clone()).collect())
		}
		QueryPlan::RingBufferScan(node) => {
			Some(node.source.def().columns.iter().map(|col| col.name.clone()).collect())
		}
		QueryPlan::SeriesScan(node) => {
			let series = node.source.def();
			let mut names = vec![series.key.column().to_string()];
			names.extend(series.data_columns().map(|col| col.name.clone()));
			Some(names)
		}
		QueryPlan::Filter(node) => output_column_names(&node.input),
		QueryPlan::Gate(node) => output_column_names(&node.input),
		QueryPlan::Take(node) => output_column_names(&node.input),
		QueryPlan::Sort(node) => output_column_names(&node.input),
		QueryPlan::Distinct(node) => output_column_names(&node.input),
		QueryPlan::Map(node) => Some(expression_labels(&node.map)),
		QueryPlan::Extend(node) => {
			let mut names = output_column_names(node.input.as_deref()?)?;
			names.extend(expression_labels(&node.extend));
			Some(names)
		}
		_ => None,
	}
}

fn expression_labels(expressions: &[Expression]) -> Vec<String> {
	expressions.iter().map(|expr| display_label(expr).text().to_string()).collect()
}

fn ensure_natural_join_shares_a_column(
	fragment: &Fragment,
	left: &QueryPlan,
	right: &QueryPlan,
	right_name: &str,
) -> Result<()> {
	let (Some(left_names), Some(right_names)) = (output_column_names(left), output_column_names(right)) else {
		return Ok(());
	};
	if left_names.iter().any(|name| right_names.contains(name)) {
		return Ok(());
	}
	let left_name = extract_source_name(left);
	Err(error!(natural_join_no_shared_column(
		fragment.clone(),
		left_name.as_deref().unwrap_or("the left input"),
		right_name
	)))
}

fn ensure_pick_columns_exist(pick: Option<&JoinPick>, right: &QueryPlan, right_name: &str) -> Result<()> {
	let (Some(pick), Some(right_names)) = (pick, output_column_names(right)) else {
		return Ok(());
	};
	let system = [ROW_NUMBER_COLUMN_NAME, CREATED_AT_COLUMN_NAME, UPDATED_AT_COLUMN_NAME, TIME_COLUMN_NAME];
	for key in &pick.keys {
		let name = key.column.text();
		if right_names.iter().any(|column| column == name)
			|| system.contains(&name.strip_prefix('#').unwrap_or(name))
		{
			continue;
		}
		return Err(error!(join_pick_column_not_found(key.column.clone(), right_name)));
	}
	Ok(())
}

fn collect_equal_conditions(expr: &Expression, out: &mut Vec<Expression>) {
	match expr {
		Expression::And(and) => {
			collect_equal_conditions(&and.left, out);
			collect_equal_conditions(&and.right, out);
		}
		other => out.push(other.clone()),
	}
}

fn extract_join_keys(conditions: &[Expression]) -> (Vec<Expression>, Vec<Expression>) {
	let mut left_keys = Vec::new();
	let mut right_keys = Vec::new();

	let mut flat = Vec::new();
	for condition in conditions {
		collect_equal_conditions(condition, &mut flat);
	}

	for condition in flat {
		match condition {
			Expression::Equal(eq) => {
				left_keys.push(*eq.left.clone());
				right_keys.push(*eq.right.clone());
			}
			_ => {
				left_keys.push(condition.clone());
				right_keys.push(condition.clone());
			}
		}
	}

	(left_keys, right_keys)
}

impl CompileOperator for JoinCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let source_name = extract_source_name(&self.right);
		let effective_alias = self.alias.or(source_name).unwrap_or_else(|| "other".to_string());

		if self.natural {
			ensure_natural_join_shares_a_column(&self.fragment, &self.left, &self.right, &effective_alias)?;
		}
		ensure_pick_columns_exist(self.pick.as_ref(), &self.right, &effective_alias)?;

		let left_node = compiler.compile_plan(txn, *self.left)?;
		let right_node = compiler.compile_plan(txn, *self.right)?;

		let (left_keys, right_keys) = extract_join_keys(&self.on);

		let node_id = compiler.add_node(
			txn,
			OperatorDef::Join {
				join_type: self.join_type,
				left: left_keys,
				right: right_keys,
				alias: Some(effective_alias),
				snapshot: self.snapshot,
				natural: self.natural,
				pick: self.pick,
			},
		)?;

		compiler.write_operator_settings_join(txn, node_id, self.retention)?;

		compiler.add_edge(txn, &left_node, &node_id)?;
		compiler.add_edge(txn, &right_node, &node_id)?;

		Ok(node_id)
	}
}
