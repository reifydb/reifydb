// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{interface::catalog::flow::OperatorId, value::column::columns::Columns};
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
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::flow::{
	flow::FlowDag,
	operator::OperatorDef::{
		Aggregate, Append, Apply, Distinct, Extend, Filter, Gate, Join, Map, SinkRingBufferView,
		SinkSeriesView, SinkSubscription, SinkTableView, Sort, SourceInlineData, SourceRingBuffer,
		SourceSeries, SourceTable, SourceView, Take, Window,
	},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, error::Error};

use crate::{
	node::{Node, SourceNode},
	txn::Lookup,
};

pub fn build<T: Lookup>(
	txn: &mut T,
	flow: &FlowDag,
	routines: &Routines,
	runtime_context: &RuntimeContext,
) -> Result<Vec<(OperatorId, Node)>> {
	let ctx = Arc::new(FlowContext::default());
	let mut nodes: Vec<(OperatorId, Node)> = Vec::new();
	for operator_id in flow.topological_order() {
		let operator = flow.get_operator(operator_id).unwrap_or_else(|| {
			panic!("transactional flow {:?} orders operator {} it does not hold", flow.id, operator_id)
		});
		let inputs = &operator.inputs;
		let node = match operator.ty.clone() {
			SourceTable {
				table,
				..
			} => Node::Source(SourceNode::new(*operator_id, txn.table(table)?.columns)),
			SourceView {
				view,
			} => Node::Source(SourceNode::new(*operator_id, txn.view(view)?.columns().to_vec())),
			Filter {
				conditions,
			} => Node::Filter(FilterOperator::new(
				parent_schema(&nodes, first_input(inputs)?)?,
				*operator_id,
				conditions,
				routines.clone(),
				runtime_context.clone(),
				Arc::clone(&ctx),
			)?),
			Map {
				expressions,
			} => Node::Map(MapOperator::new(
				parent_schema(&nodes, first_input(inputs)?)?,
				*operator_id,
				expressions,
				routines.clone(),
				runtime_context.clone(),
				Arc::clone(&ctx),
			)?),
			Extend {
				expressions,
			} => Node::Extend(ExtendOperator::new(
				parent_schema(&nodes, first_input(inputs)?)?,
				*operator_id,
				expressions,
				routines.clone(),
				runtime_context.clone(),
				Arc::clone(&ctx),
			)?),
			Append {} => {
				if inputs.len() != 2 {
					return Err(Error::from(FlowGraphError::NodeInputArity {
						operator: "Append",
						expected: "exactly 2",
						found: inputs.len(),
					}));
				}

				let mut parent_schemas = Vec::with_capacity(inputs.len());
				for input_node_id in inputs {
					parent_schemas.push(parent_schema(&nodes, *input_node_id)?);
				}

				let parent_schema = parent_schemas.swap_remove(0);
				let lanes = assign_lanes(flow, *operator_id)?;
				Node::Append(AppendOperator::new(*operator_id, parent_schema, inputs.to_vec(), lanes))
			}
			Sort {
				..
			} => Node::Sort(*operator_id, parent_schema(&nodes, first_input(inputs)?)?),
			SinkTableView {
				..
			} => continue,
			SourceInlineData {
				..
			}
			| SourceRingBuffer {
				..
			}
			| SourceSeries {
				..
			}
			| Gate {
				..
			}
			| Join {
				..
			}
			| Aggregate {
				..
			}
			| Take {
				..
			}
			| Distinct {
				..
			}
			| Apply {
				..
			}
			| SinkRingBufferView {
				..
			}
			| SinkSeriesView {
				..
			}
			| SinkSubscription {
				..
			}
			| Window {
				..
			} => panic!("transactional flow {:?} holds unsupported operator {}", flow.id, operator_id),
		};
		nodes.push((*operator_id, node));
	}
	Ok(nodes)
}

fn require_parent(nodes: &[(OperatorId, Node)], input: OperatorId) -> Result<&Node> {
	nodes.iter().find(|(id, _)| *id == input).map(|(_, node)| node).ok_or_else(|| {
		Error::from(FlowGraphError::ParentOperatorNotFound {
			input: format!("{:?}", input),
		})
	})
}

fn parent_schema(nodes: &[(OperatorId, Node)], input: OperatorId) -> Result<Option<Columns>> {
	Ok(require_parent(nodes, input)?.output_schema())
}

fn first_input(inputs: &[OperatorId]) -> Result<OperatorId> {
	inputs.first().copied().ok_or_else(|| Error::from(FlowGraphError::MissingInputEdge))
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion, TimeDomain, TimeSource},
		interface::{
			catalog::{
				column::{Column, ColumnIndex},
				flow::{FlowId, OperatorId},
				id::{ColumnId, NamespaceId, TableId, ViewId},
				object::ObjectId,
				table::Table,
				view::{TableView, View, ViewKind},
			},
			change::{Change, ChangeOrigin, Diff},
		},
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_routine_abi::registry::Routines;
	use reifydb_rql::{
		expression::{Expression, parse_expression},
		flow::{
			flow::{FlowBuilder, FlowDag},
			operator::{FlowEdge, FlowNode, OperatorDef},
		},
	};
	use reifydb_runtime::context::{
		RuntimeContext,
		clock::{Clock, MockClock},
	};
	use reifydb_value::{
		factory::time::at_millis,
		fragment::Fragment,
		value::{
			constraint::TypeConstraint, row_number::RowNumber, system_columns::SystemColumns,
			value_type::ValueType,
		},
	};

	use super::build;
	use crate::memory::MemoryTxn;

	const TABLE: TableId = TableId(1);
	const VIEW: ViewId = ViewId(2);
	const SINK: ViewId = ViewId(9);

	fn columns() -> Vec<Column> {
		vec![Column {
			id: ColumnId(1),
			name: "v".to_string(),
			constraint: TypeConstraint::unconstrained(ValueType::Int8),
			properties: Vec::new(),
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		}]
	}

	fn memory_txn() -> MemoryTxn {
		let mut txn = MemoryTxn::default();
		txn.tables.insert(
			TABLE,
			Table {
				id: TABLE,
				namespace: NamespaceId(1),
				name: "t".to_string(),
				columns: columns(),
				primary_key: None,
				partition_by: Vec::new(),
				time: TimeSource::None,
			},
		);
		txn.views.insert(
			VIEW,
			View::Table(TableView {
				id: VIEW,
				namespace: NamespaceId(1),
				name: "u".to_string(),
				kind: ViewKind::Transactional,
				columns: columns(),
				primary_key: None,
				partition_by: Vec::new(),
				sort: Vec::new(),
			}),
		);
		txn
	}

	fn flow(nodes: Vec<(u64, OperatorDef)>, edges: &[(u64, u64)]) -> FlowDag {
		let mut builder = FlowBuilder::new(FlowId(1));
		for (id, ty) in nodes {
			builder.add_node(FlowNode::new(id, ty));
		}
		for (position, (source, target)) in edges.iter().enumerate() {
			builder.add_edge(FlowEdge::new(position as u64, *source, *target)).unwrap();
		}
		builder.build()
	}

	fn expressions(rql: &[&str]) -> Vec<Expression> {
		rql.iter().flat_map(|text| parse_expression(text).expect("the expression parses")).collect()
	}

	fn runtime_context() -> RuntimeContext {
		RuntimeContext::with_clock(Clock::Mock(MockClock::from_millis(0)))
	}

	fn source_table() -> OperatorDef {
		OperatorDef::SourceTable {
			table: TABLE,
			time_domain: TimeDomain::None,
		}
	}

	fn sink() -> OperatorDef {
		OperatorDef::SinkTableView {
			view: SINK,
		}
	}

	fn rows(rows: &[(u64, i64)]) -> Columns {
		let n = rows.len();
		Columns::with_system(
			vec![ColumnWithName::new(
				Fragment::internal("v"),
				ColumnBuffer::int8(rows.iter().map(|(_, v)| *v).collect::<Vec<_>>()),
			)],
			SystemColumns::new(
				rows.iter().map(|(row, _)| RowNumber(*row)).collect(),
				Vec::new(),
				vec![at_millis(10); n],
				vec![at_millis(20); n],
				vec![at_millis(30); n],
				Vec::new(),
			),
		)
	}

	#[test]
	fn build_returns_every_parent_before_its_children_and_leaves_the_sink_out() {
		let flow = flow(
			vec![
				(1, sink()),
				(
					2,
					OperatorDef::Filter {
						conditions: expressions(&["v > 50"]),
					},
				),
				(
					3,
					OperatorDef::SourceView {
						view: VIEW,
					},
				),
				(5, source_table()),
				(7, OperatorDef::Append {}),
			],
			&[(5, 7), (3, 7), (7, 2), (2, 1)],
		);
		let expected: Vec<OperatorId> =
			flow.topological_order().iter().copied().filter(|id| *id != OperatorId(1)).collect();

		let nodes = build(&mut memory_txn(), &flow, &Routines::empty(), &runtime_context()).unwrap();

		assert_eq!(expected.len(), 4);
		assert_eq!(nodes.iter().map(|(id, _)| *id).collect::<Vec<_>>(), expected);
		assert!(nodes.iter().all(|(id, node)| node.id() == *id));
	}

	#[test]
	fn a_source_filter_map_chain_drops_the_rows_the_filter_refuses_and_keeps_row_numbers() {
		let flow = flow(
			vec![
				(1, source_table()),
				(
					2,
					OperatorDef::Filter {
						conditions: expressions(&["v > 50"]),
					},
				),
				(
					3,
					OperatorDef::Map {
						expressions: expressions(&["v", "doubled: v * 2"]),
					},
				),
				(4, sink()),
			],
			&[(1, 2), (2, 3), (3, 4)],
		);
		let mut txn = memory_txn();
		let mut nodes = build(&mut txn, &flow, &Routines::empty(), &runtime_context()).unwrap();
		let mut change = Change::from_object(
			ObjectId::table(TABLE),
			ChangeVersion::from(CommitVersion(1)),
			vec![Diff::insert(rows(&[(1, 10), (2, 60), (3, 70)]))],
			at_millis(0),
		);

		for (_, node) in nodes.iter_mut() {
			change = node.apply(&mut txn, change).unwrap();
		}

		assert_eq!(change.origin, ChangeOrigin::Flow(OperatorId(3)));
		assert_eq!(change.diffs.len(), 1);
		let Diff::Insert {
			post,
			..
		} = &change.diffs[0]
		else {
			panic!("an insert must reach the end of the chain as an insert: {:?}", change.diffs[0]);
		};
		assert_eq!(post.row_numbers(), &[RowNumber(2), RowNumber(3)]);
		assert_eq!(post.name_at(1).text(), "doubled");
		assert_eq!(
			(0..post.row_count()).map(|row| post[1].get_value(row).to_string()).collect::<Vec<_>>(),
			vec!["120", "140"]
		);
	}

	#[test]
	fn a_sort_reports_its_parents_schema_so_operators_below_it_keep_the_column_types() {
		let flow = flow(
			vec![
				(1, source_table()),
				(
					2,
					OperatorDef::Sort {
						by: Vec::new(),
					},
				),
				(3, sink()),
			],
			&[(1, 2), (2, 3)],
		);

		let nodes = build(&mut memory_txn(), &flow, &Routines::empty(), &runtime_context()).unwrap();

		let source = nodes[0].1.output_schema();
		let sort = nodes[1].1.output_schema();
		assert!(source.is_some());
		assert_eq!(format!("{:?}", sort), format!("{:?}", source));
	}

	#[test]
	fn build_rejects_a_sort_without_an_input_edge_instead_of_sorting_nothing() {
		let flow = flow(
			vec![(
				1,
				OperatorDef::Sort {
					by: Vec::new(),
				},
			)],
			&[],
		);

		let Err(err) = build(&mut memory_txn(), &flow, &Routines::empty(), &runtime_context()) else {
			panic!("a sort with no input edge must not build");
		};

		assert_eq!(err.code, "FLOW_028");
	}

	#[test]
	#[should_panic(expected = "transactional flow FlowId(1) holds unsupported operator 2")]
	fn build_refuses_an_operator_transactional_views_cannot_run() {
		let flow = flow(
			vec![
				(1, source_table()),
				(
					2,
					OperatorDef::Take {
						limit: 1,
					},
				),
				(3, sink()),
			],
			&[(1, 2), (2, 3)],
		);

		build(&mut memory_txn(), &flow, &Routines::empty(), &runtime_context()).unwrap();
	}
}
