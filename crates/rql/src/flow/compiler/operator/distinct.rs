// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		identifier::{ColumnIdentifier, ColumnObject},
		resolved::{ResolvedColumn, ResolvedObject},
	},
	row::OperatorTtl,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, fragment::Fragment};

use crate::{
	expression::{ColumnExpression, Expression},
	flow::{
		compiler::{CompileOperator, FlowCompiler},
		operator::OperatorDef::Distinct,
	},
	nodes::DistinctNode,
	query::QueryPlan,
};

pub(crate) struct DistinctCompiler {
	pub input: Box<QueryPlan>,
	pub columns: Vec<ResolvedColumn>,
	pub ttl: Option<OperatorTtl>,
}

impl From<DistinctNode> for DistinctCompiler {
	fn from(node: DistinctNode) -> Self {
		Self {
			input: node.input,
			columns: node.columns.into_iter().collect(),
			ttl: node.ttl,
		}
	}
}

fn resolved_to_column_identifier(resolved: ResolvedColumn) -> ColumnIdentifier {
	let object = match resolved.object() {
		ResolvedObject::Table(t) => ColumnObject::Qualified {
			namespace: Fragment::internal(t.namespace().name()),
			name: Fragment::internal(t.name()),
		},
		ResolvedObject::View(v) => ColumnObject::Qualified {
			namespace: Fragment::internal(v.namespace().name()),
			name: Fragment::internal(v.name()),
		},
		ResolvedObject::RingBuffer(r) => ColumnObject::Qualified {
			namespace: Fragment::internal(r.namespace().name()),
			name: Fragment::internal(r.name()),
		},
		_ => ColumnObject::Alias(Fragment::internal("_unknown")),
	};

	ColumnIdentifier {
		object,
		name: Fragment::internal(resolved.name()),
	}
}

impl CompileOperator for DistinctCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let input_node = compiler.compile_plan(txn, *self.input)?;

		let expressions: Vec<Expression> = self
			.columns
			.into_iter()
			.map(|col| Expression::Column(ColumnExpression(resolved_to_column_identifier(col))))
			.collect();

		let node_id = compiler.add_node(
			txn,
			Distinct {
				expressions,
			},
		)?;

		compiler.write_operator_settings(txn, node_id, self.ttl)?;

		compiler.add_edge(txn, &input_node, &node_id)?;
		Ok(node_id)
	}
}
