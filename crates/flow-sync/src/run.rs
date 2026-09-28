// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::{flow::OperatorId, id::ViewId, object::ObjectId},
		change::{Change, Diff},
		consolidate::consolidate_diffs,
	},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::flow::{
	flow::FlowDag,
	operator::OperatorDef::{SinkTableView, SourceTable, SourceView},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, value::datetime::DateTime};

use crate::{
	graph::build,
	node::Node,
	txn::{Changes, ClockNow, Emit, Intern, Lookup, Rows},
};

pub fn run<T: Changes + Rows + Emit + Lookup + Intern + ClockNow>(
	txn: &mut T,
	routines: &Routines,
	runtime_context: &RuntimeContext,
) -> Result<()> {
	let at = txn.cursor();
	if txn.entries_from(at).is_empty() {
		return Ok(());
	}

	let flows = order_flows(txn.transactional_flows()?);
	for flow in &flows {
		let mut nodes = build(txn, flow, routines, runtime_context)?;
		let entries = txn.entries_from(at);
		let pending = seed_entry_nodes(flow, &entries, txn.now())?;
		if pending.is_empty() {
			continue;
		}
		run_topology(txn, flow, &mut nodes, pending)?;
	}

	let processed = txn.entries_from(at).len();
	txn.set_cursor(at + processed);
	Ok(())
}

fn order_flows(mut flows: Vec<FlowDag>) -> Vec<FlowDag> {
	flows.sort_by_key(|flow| flow.id);
	let mut ordered = Vec::with_capacity(flows.len());
	while !flows.is_empty() {
		let ready = flows
			.iter()
			.position(|reader| !flows.iter().any(|writer| feeds(writer, reader)))
			.unwrap_or_else(|| {
				panic!(
					"transactional flows {:?} cannot be ordered: their views form a cycle",
					flows.iter().map(|flow| flow.id).collect::<Vec<_>>()
				)
			});
		ordered.push(flows.remove(ready));
	}
	ordered
}

fn feeds(writer: &FlowDag, reader: &FlowDag) -> bool {
	let written: Vec<ViewId> = writer
		.get_operator_ids()
		.filter_map(|id| match writer.get_operator(&id).map(|operator| &operator.ty) {
			Some(SinkTableView {
				view,
			}) => Some(*view),
			_ => None,
		})
		.collect();
	reader.get_operator_ids().any(|id| {
		reader.get_operator(&id)
			.is_some_and(|operator| matches!(&operator.ty, SourceView { view } if written.contains(view)))
	})
}

fn seed_entry_nodes(
	flow: &FlowDag,
	entries: &[(ObjectId, Diff)],
	changed_at: DateTime,
) -> Result<HashMap<OperatorId, Vec<Change>>> {
	let mut pending: HashMap<OperatorId, Vec<Change>> = HashMap::new();
	for operator_id in flow.topological_order() {
		let operator = flow.get_operator(operator_id).unwrap_or_else(|| {
			panic!("transactional flow {:?} orders operator {} it does not hold", flow.id, operator_id)
		});
		let object = match &operator.ty {
			SourceTable {
				table,
				..
			} => ObjectId::table(*table),
			SourceView {
				view,
			} => ObjectId::view(*view),
			_ => continue,
		};
		let diffs = consolidate_diffs(
			entries.iter().filter(|(entry, _)| *entry == object).map(|(_, diff)| diff.clone()).collect(),
		)?;
		if diffs.is_empty() {
			continue;
		}
		pending.insert(
			*operator_id,
			vec![Change::from_object(object, ChangeVersion::from(CommitVersion(0)), diffs, changed_at)],
		);
	}
	Ok(pending)
}

fn run_topology<T: Rows + Emit + Lookup + Intern>(
	txn: &mut T,
	flow: &FlowDag,
	nodes: &mut [(OperatorId, Node)],
	mut pending: HashMap<OperatorId, Vec<Change>>,
) -> Result<()> {
	for (operator_id, node) in nodes.iter_mut() {
		let inbox = match pending.remove(operator_id) {
			Some(v) if !v.is_empty() => v,
			_ => continue,
		};

		let operator = flow.get_operator(operator_id).unwrap_or_else(|| {
			panic!("transactional flow {:?} built operator {} it does not hold", flow.id, operator_id)
		});

		let combined_output = dispatch_node(txn, *operator_id, node, inbox)?;
		if combined_output.diffs.is_empty() {
			continue;
		}

		for child_id in &operator.outputs {
			pending.entry(*child_id).or_default().push(combined_output.clone());
		}
	}
	Ok(())
}

fn dispatch_node<T: Rows + Emit + Lookup + Intern>(
	txn: &mut T,
	operator_id: OperatorId,
	node: &mut Node,
	inbox: Vec<Change>,
) -> Result<Change> {
	let merged = Change::merge(inbox)?;
	let version = merged.version;
	let changed_at = merged.changed_at;
	let result = node.apply(txn, merged)?;
	Ok(Change::from_flow(operator_id, version, result.diffs, changed_at.max(result.changed_at)))
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeSet;

	use reifydb_codec::row::shape::RowFamily;
	use reifydb_core::{
		common::{TimeDomain, TimeSource},
		interface::{
			catalog::{
				column::{Column, ColumnIndex},
				flow::FlowId,
				id::{ColumnId, NamespaceId, TableId, ViewId},
				object::ObjectId,
				table::Table,
				view::{TableView, View, ViewKind},
			},
			change::Diff,
		},
		row::row_shape_from_columns,
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_flow::operator::sink::view::row_key;
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
			Value, constraint::TypeConstraint, row_number::RowNumber, system_columns::SystemColumns,
			value_type::ValueType,
		},
	};

	use super::run;
	use crate::memory::MemoryTxn;

	const TABLE: TableId = TableId(1);
	const VIEW: ViewId = ViewId(2);
	const DOWNSTREAM: ViewId = ViewId(3);

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

	fn transactional_view(id: ViewId) -> View {
		View::Table(TableView {
			id,
			namespace: NamespaceId(1),
			name: format!("view_{}", id.0),
			kind: ViewKind::Transactional,
			columns: columns(),
			primary_key: None,
			partition_by: Vec::new(),
			sort: Vec::new(),
		})
	}

	fn memory_txn(flows: Vec<FlowDag>) -> MemoryTxn {
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
		txn.views.insert(VIEW, transactional_view(VIEW));
		txn.views.insert(DOWNSTREAM, transactional_view(DOWNSTREAM));
		txn.flows = flows;
		txn
	}

	fn flow(id: u64, nodes: Vec<(u64, OperatorDef)>, edges: &[(u64, u64)]) -> FlowDag {
		let mut builder = FlowBuilder::new(FlowId(id));
		for (node, ty) in nodes {
			builder.add_node(FlowNode::new(node, ty));
		}
		for (position, (source, target)) in edges.iter().enumerate() {
			builder.add_edge(FlowEdge::new(position as u64, *source, *target)).unwrap();
		}
		builder.build()
	}

	fn expressions(rql: &[&str]) -> Vec<Expression> {
		rql.iter().flat_map(|text| parse_expression(text).expect("the expression parses")).collect()
	}

	fn filter(rql: &str) -> OperatorDef {
		OperatorDef::Filter {
			conditions: expressions(&[rql]),
		}
	}

	fn source_table() -> OperatorDef {
		OperatorDef::SourceTable {
			table: TABLE,
			time_domain: TimeDomain::None,
		}
	}

	fn sink(view: ViewId) -> OperatorDef {
		OperatorDef::SinkTableView {
			view,
		}
	}

	fn table_into_view(id: u64) -> FlowDag {
		flow(id, vec![(1, source_table()), (2, filter("v > 50")), (3, sink(VIEW))], &[(1, 2), (2, 3)])
	}

	fn view_into_downstream(id: u64) -> FlowDag {
		flow(
			id,
			vec![
				(
					1,
					OperatorDef::SourceView {
						view: VIEW,
					},
				),
				(2, filter("v > 60")),
				(3, sink(DOWNSTREAM)),
			],
			&[(1, 2), (2, 3)],
		)
	}

	fn runtime_context() -> RuntimeContext {
		RuntimeContext::with_clock(Clock::Mock(MockClock::from_millis(0)))
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
				Vec::new(),
				Vec::new(),
			),
		)
	}

	fn table_entry(diff: Diff) -> (ObjectId, Diff) {
		(ObjectId::table(TABLE), diff)
	}

	fn run_all(txn: &mut MemoryTxn) {
		run(txn, &Routines::empty(), &runtime_context()).unwrap();
	}

	fn stored(txn: &MemoryTxn, view: ViewId, row: u64) -> Option<Value> {
		let shape = row_shape_from_columns(RowFamily::Table, &columns());
		txn.rows.get(&row_key(transactional_view(view).storage_id(), RowNumber(row)))
			.map(|bytes| shape.get_value(bytes, 0))
	}

	fn row_numbers(diff: &Diff) -> Vec<RowNumber> {
		diff.post().or(diff.pre()).expect("every diff carries columns").row_numbers().to_vec()
	}

	fn emitted_to(txn: &MemoryTxn, view: ViewId) -> Vec<&Diff> {
		txn.emitted.iter().filter(|(target, _)| *target == view).map(|(_, diff)| diff).collect()
	}

	#[test]
	fn table_inserts_updates_and_removes_reach_the_view_through_a_filter_as_rows_and_emitted_diffs() {
		let mut txn = memory_txn(vec![table_into_view(1)]);
		txn.entries.push(table_entry(Diff::insert(rows(&[(1, 10), (2, 60), (3, 70)]))));

		run_all(&mut txn);

		assert_eq!(txn.rows.len(), 2);
		assert_eq!(stored(&txn, VIEW, 2), Some(Value::Int8(60)));
		assert_eq!(stored(&txn, VIEW, 3), Some(Value::Int8(70)));
		assert_eq!(txn.emitted.len(), 1);
		assert!(matches!(txn.emitted[0].1, Diff::Insert { .. }));
		assert_eq!(row_numbers(&txn.emitted[0].1), vec![RowNumber(2), RowNumber(3)]);

		txn.entries.push(table_entry(Diff::update(rows(&[(2, 60)]), rows(&[(2, 65)]))));
		txn.entries.push(table_entry(Diff::remove(rows(&[(3, 70)]))));

		run_all(&mut txn);

		assert_eq!(txn.rows.len(), 1);
		assert_eq!(stored(&txn, VIEW, 2), Some(Value::Int8(65)));
		assert_eq!(stored(&txn, VIEW, 3), None);
		let second_pass: Vec<&Diff> = txn.emitted[1..].iter().map(|(_, diff)| diff).collect();
		assert_eq!(second_pass.len(), 2);
		assert!(second_pass
			.iter()
			.any(|diff| matches!(diff, Diff::Update { .. }) && row_numbers(diff) == vec![RowNumber(2)]));
		assert!(second_pass
			.iter()
			.any(|diff| matches!(diff, Diff::Remove { .. }) && row_numbers(diff) == vec![RowNumber(3)]));
	}

	#[test]
	fn a_view_reading_another_transactional_view_is_maintained_in_the_same_pass_even_when_listed_first() {
		let mut txn = memory_txn(vec![view_into_downstream(1), table_into_view(2)]);
		txn.entries.push(table_entry(Diff::insert(rows(&[(1, 10), (2, 60), (3, 70)]))));

		run_all(&mut txn);

		assert_eq!(txn.emitted.iter().map(|(view, _)| *view).collect::<Vec<_>>(), vec![VIEW, DOWNSTREAM]);
		assert_eq!(stored(&txn, DOWNSTREAM, 3), Some(Value::Int8(70)));
		assert_eq!(stored(&txn, DOWNSTREAM, 2), None);
		assert_eq!(row_numbers(emitted_to(&txn, DOWNSTREAM)[0]), vec![RowNumber(3)]);
	}

	#[test]
	fn an_append_diamond_writes_one_view_row_per_branch_and_source_row_and_emits_each_once() {
		let diamond = flow(
			1,
			vec![
				(1, source_table()),
				(2, filter("v > 0")),
				(3, filter("v > 5")),
				(4, OperatorDef::Append {}),
				(5, sink(VIEW)),
			],
			&[(1, 2), (1, 3), (2, 4), (3, 4), (4, 5)],
		);
		let mut txn = memory_txn(vec![diamond]);
		txn.entries.push(table_entry(Diff::insert(rows(&[(1, 10), (2, 20)]))));

		run_all(&mut txn);

		let emitted: Vec<RowNumber> = emitted_to(&txn, VIEW).into_iter().flat_map(row_numbers).collect();
		let distinct: BTreeSet<RowNumber> = emitted.iter().copied().collect();
		assert_eq!(emitted.len(), 4);
		assert_eq!(distinct.len(), 4);
		assert_eq!(txn.rows.len(), 4);
		for row in distinct {
			assert!(stored(&txn, VIEW, row.0).is_some(), "emitted row {} must be stored", row.0);
		}
	}

	#[test]
	fn run_reads_only_entries_past_the_cursor_and_leaves_it_past_the_view_diffs_it_emitted() {
		let mut txn = memory_txn(vec![table_into_view(1)]);
		txn.entries.push(table_entry(Diff::insert(rows(&[(9, 90)]))));
		txn.cursor = 1;
		txn.entries.push(table_entry(Diff::insert(rows(&[(1, 60)]))));

		run_all(&mut txn);

		assert_eq!(txn.entries.len(), 3);
		assert_eq!(txn.entries[2].0, ObjectId::view(VIEW));
		assert_eq!(txn.cursor, 3);
		assert_eq!(stored(&txn, VIEW, 9), None);
		assert_eq!(stored(&txn, VIEW, 1), Some(Value::Int8(60)));
	}

	#[test]
	fn a_second_run_without_new_entries_writes_and_emits_nothing() {
		let mut txn = memory_txn(vec![table_into_view(1), view_into_downstream(2)]);
		txn.entries.push(table_entry(Diff::insert(rows(&[(1, 70)]))));
		run_all(&mut txn);
		let rows_before = txn.rows.clone();
		let emitted_before = txn.emitted.len();
		let entries_before = txn.entries.len();

		run_all(&mut txn);

		assert_eq!(txn.rows, rows_before);
		assert_eq!(txn.emitted.len(), emitted_before);
		assert_eq!(txn.entries.len(), entries_before);
		assert_eq!(txn.cursor, entries_before);
	}

	#[test]
	fn entries_of_an_object_no_flow_reads_are_skipped_without_error() {
		let mut txn = memory_txn(vec![table_into_view(1)]);
		txn.entries.push((ObjectId::table(TableId(99)), Diff::insert(rows(&[(1, 70)]))));

		run_all(&mut txn);

		assert!(txn.rows.is_empty());
		assert!(txn.emitted.is_empty());
		assert_eq!(txn.cursor, 1);
	}
}
