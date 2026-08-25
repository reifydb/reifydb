// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_core::{
	common::TimeDomain,
	interface::catalog::{flow::FlowId, id::TableId},
	value::column::columns::Columns,
};
use reifydb_rql::flow::{
	flow::{FlowBuilder, FlowDag},
	operator::{FlowEdge, FlowNode, OperatorDef},
};

use super::{lane::*, *};

fn lanes(bits: u32, stamps: [Option<u64>; 2]) -> AppendLanes {
	AppendLanes::new(OperatorId(1), bits, stamps)
}

fn op(bits: u32, stamps: [Option<u64>; 2]) -> AppendOperator {
	AppendOperator::new_for_state_tests(OperatorId(1), lanes(bits, stamps))
}

fn rows(source_rows: &[u64]) -> Columns {
	Columns::empty().with_row_numbers(source_rows.iter().map(|r| RowNumber(*r)).collect())
}

fn numbers(columns: &Columns) -> Vec<u64> {
	columns.row_numbers().iter().map(|r| r.0).collect()
}

#[test]
fn a_source_row_translates_to_its_lane_stamped_output_row() {
	let mut operator = op(1, [Some(0), Some(1)]);

	let diff = operator.translate_append_insert(0, rows(&[42])).expect("an insert must translate");

	match diff {
		Diff::Insert {
			post,
			..
		} => assert_eq!(numbers(&post), vec![84]),
		other => panic!("expected an insert, found {other:?}"),
	}
}

#[test]
fn the_same_source_row_always_translates_to_the_same_output_row() {
	let mut operator = op(1, [Some(0), Some(1)]);

	let inserted = operator.translate_append_insert(0, rows(&[42])).expect("an insert must translate");
	let removed = operator.translate_append_remove(0, rows(&[42])).expect("a remove must translate");

	let (
		Diff::Insert {
			post,
			..
		},
		Diff::Remove {
			pre,
			..
		},
	) = (inserted, removed)
	else {
		panic!("expected an insert and a remove");
	};
	assert_eq!(numbers(&post), numbers(&pre));
}

#[test]
fn each_input_numbers_its_own_source_rows_independently() {
	let mut operator = op(1, [Some(0), Some(1)]);

	let left = operator.translate_append_insert(0, rows(&[7])).expect("an insert must translate");
	let right = operator.translate_append_insert(1, rows(&[7])).expect("an insert must translate");

	let (
		Diff::Insert {
			post: left,
			..
		},
		Diff::Insert {
			post: right,
			..
		},
	) = (left, right)
	else {
		panic!("expected two inserts");
	};
	assert_eq!(numbers(&left), vec![14]);
	assert_eq!(numbers(&right), vec![15]);
	assert_ne!(numbers(&left), numbers(&right));
}

#[test]
fn a_source_row_repeated_inside_one_batch_lands_on_one_output_row() {
	let mut operator = op(1, [Some(0), Some(1)]);

	let diff = operator.translate_append_insert(0, rows(&[5, 5, 5])).expect("an insert must translate");

	let Diff::Insert {
		post,
		..
	} = diff
	else {
		panic!("expected an insert");
	};
	assert_eq!(numbers(&post), vec![10, 10, 10]);
}

#[test]
fn a_batch_keeps_every_slot_aligned_with_its_source_row() {
	let mut operator = op(2, [Some(0), Some(2)]);

	let diff = operator.translate_append_insert(1, rows(&[1, 9, 4])).expect("an insert must translate");

	let Diff::Insert {
		post,
		..
	} = diff
	else {
		panic!("expected an insert");
	};
	assert_eq!(numbers(&post), vec![6, 38, 18]);
}

#[test]
fn an_update_carries_the_same_output_row_on_both_sides() {
	let mut operator = op(1, [Some(0), Some(1)]);

	let diff = operator.translate_append_update(0, rows(&[3]), rows(&[3])).expect("an update must translate");

	let Diff::Update {
		pre,
		post,
		..
	} = diff
	else {
		panic!("expected an update");
	};
	assert_eq!(numbers(&pre), vec![6]);
	assert_eq!(numbers(&post), vec![6]);
}

#[test]
fn a_retraction_translates_no_matter_how_long_it_waited() {
	let mut operator = op(1, [Some(0), Some(1)]);

	let diff = operator.translate_append_remove(0, rows(&[42])).expect("a remove must translate");

	let Diff::Remove {
		pre,
		..
	} = diff
	else {
		panic!("expected a remove");
	};
	assert_eq!(numbers(&pre), vec![84]);
}

#[test]
fn an_empty_batch_translates_to_nothing() {
	let mut operator = op(1, [Some(0), Some(1)]);

	assert!(operator.translate_append_insert(0, rows(&[])).is_none());
	assert!(operator.translate_append_remove(0, rows(&[])).is_none());
}

#[test]
fn append_reports_no_operator_sample() {
	assert!(HostOperator::sample(&op(1, [Some(0), Some(1)])).is_none());
}

struct Dag {
	builder: FlowBuilder,
	next_edge: u64,
}

impl Dag {
	fn new() -> Self {
		Self {
			builder: FlowDag::builder(FlowId(1)),
			next_edge: 0,
		}
	}

	fn node(&mut self, id: u64, ty: OperatorDef) -> OperatorId {
		let operator = OperatorId(id);
		self.builder.add_node(FlowNode::new(operator, ty));
		operator
	}

	fn source(&mut self, id: u64) -> OperatorId {
		self.node(
			id,
			OperatorDef::SourceTable {
				table: TableId(id),
				time_domain: TimeDomain::None,
			},
		)
	}

	fn append(&mut self, id: u64, left: OperatorId, right: OperatorId) -> OperatorId {
		let operator = self.node(id, OperatorDef::Append {});
		self.edge(left, operator);
		self.edge(right, operator);
		operator
	}

	fn edge(&mut self, from: OperatorId, to: OperatorId) {
		self.next_edge += 1;
		self.builder.add_edge(FlowEdge::new(self.next_edge, from, to)).unwrap();
	}

	fn build(self) -> FlowDag {
		self.builder.build()
	}
}

#[test]
fn a_two_branch_chain_uses_one_lane_bit_and_stamps_both_sides() {
	let mut dag = Dag::new();
	let a = dag.source(1);
	let b = dag.source(2);
	let append = dag.append(10, a, b);
	let dag = dag.build();

	let lanes = assign_lanes(&dag, append).unwrap();

	assert_eq!(lanes.bits(), 1);
	assert_eq!(lanes.stamps(), &[Some(0), Some(1)]);
}

#[test]
fn every_node_in_a_three_branch_chain_agrees_on_two_lane_bits() {
	let mut dag = Dag::new();
	let a = dag.source(1);
	let b = dag.source(2);
	let c = dag.source(3);
	let inner = dag.append(10, a, b);
	let outer = dag.append(11, inner, c);
	let dag = dag.build();

	let inner_lanes = assign_lanes(&dag, inner).unwrap();
	let outer_lanes = assign_lanes(&dag, outer).unwrap();

	assert_eq!(inner_lanes.bits(), 2);
	assert_eq!(outer_lanes.bits(), 2);
	assert_eq!(inner_lanes.stamps(), &[Some(0), Some(1)]);
	assert_eq!(outer_lanes.stamps(), &[None, Some(2)]);
}

#[test]
fn a_balanced_tree_of_four_branches_uses_two_lane_bits() {
	let mut dag = Dag::new();
	let a = dag.source(1);
	let b = dag.source(2);
	let c = dag.source(3);
	let d = dag.source(4);
	let left = dag.append(10, a, b);
	let right = dag.append(11, c, d);
	let root = dag.append(12, left, right);
	let dag = dag.build();

	assert_eq!(assign_lanes(&dag, left).unwrap().bits(), 2);
	assert_eq!(assign_lanes(&dag, root).unwrap().stamps(), &[None, None]);
	assert_eq!(assign_lanes(&dag, left).unwrap().stamps(), &[Some(0), Some(1)]);
	assert_eq!(assign_lanes(&dag, right).unwrap().stamps(), &[Some(2), Some(3)]);
}

#[test]
fn a_map_between_two_appends_keeps_them_in_one_chain() {
	let mut dag = Dag::new();
	let a = dag.source(1);
	let b = dag.source(2);
	let c = dag.source(3);
	let inner = dag.append(10, a, b);
	let mapped = dag.node(
		20,
		OperatorDef::Map {
			expressions: Vec::new(),
		},
	);
	dag.edge(inner, mapped);
	let outer = dag.append(11, mapped, c);
	let dag = dag.build();

	let outer_lanes = assign_lanes(&dag, outer).unwrap();

	assert_eq!(outer_lanes.bits(), 2);
	assert_eq!(outer_lanes.stamps(), &[None, Some(2)]);
}

#[test]
fn a_distinct_between_two_appends_splits_them_into_separate_chains() {
	let mut dag = Dag::new();
	let a = dag.source(1);
	let b = dag.source(2);
	let c = dag.source(3);
	let inner = dag.append(10, a, b);
	let distinct = dag.node(
		20,
		OperatorDef::Distinct {
			expressions: Vec::new(),
		},
	);
	dag.edge(inner, distinct);
	let outer = dag.append(11, distinct, c);
	let dag = dag.build();

	let inner_lanes = assign_lanes(&dag, inner).unwrap();
	let outer_lanes = assign_lanes(&dag, outer).unwrap();

	assert_eq!(inner_lanes.bits(), 1);
	assert_eq!(inner_lanes.stamps(), &[Some(0), Some(1)]);
	assert_eq!(outer_lanes.bits(), 1);
	assert_eq!(outer_lanes.stamps(), &[Some(0), Some(1)]);
}

#[test]
fn stamping_is_injective_across_a_three_branch_chain() {
	let mut dag = Dag::new();
	let a = dag.source(1);
	let b = dag.source(2);
	let c = dag.source(3);
	let inner = dag.append(10, a, b);
	let outer = dag.append(11, inner, c);
	let dag = dag.build();

	let inner_lanes = assign_lanes(&dag, inner).unwrap();
	let outer_lanes = assign_lanes(&dag, outer).unwrap();

	let mut seen = HashSet::new();
	for source in 1..200u64 {
		assert!(seen.insert(inner_lanes.stamp(0, RowNumber(source)).0));
		assert!(seen.insert(inner_lanes.stamp(1, RowNumber(source)).0));
		assert!(seen.insert(outer_lanes.stamp(1, RowNumber(source)).0));
	}
}

#[test]
fn every_branch_scales_by_the_same_factor() {
	let mut dag = Dag::new();
	let a = dag.source(1);
	let b = dag.source(2);
	let c = dag.source(3);
	let inner = dag.append(10, a, b);
	let outer = dag.append(11, inner, c);
	let dag = dag.build();

	let inner_lanes = assign_lanes(&dag, inner).unwrap();
	let outer_lanes = assign_lanes(&dag, outer).unwrap();

	assert_eq!(inner_lanes.stamp(0, RowNumber(1)), RowNumber(4));
	assert_eq!(inner_lanes.stamp(1, RowNumber(1)), RowNumber(5));
	assert_eq!(outer_lanes.stamp(1, RowNumber(1)), RowNumber(6));
	assert_eq!(inner_lanes.stamp(0, RowNumber(2)), RowNumber(8));
	assert_eq!(outer_lanes.stamp(1, RowNumber(2)), RowNumber(10));
}

#[test]
fn a_pass_through_side_returns_the_source_row_unchanged() {
	let lanes = AppendLanes::new(OperatorId(1), 2, [None, Some(3)]);

	assert_eq!(lanes.stamp(0, RowNumber(9)), RowNumber(9));
	assert_eq!(lanes.stamp(1, RowNumber(9)), RowNumber(39));
}

#[test]
fn a_stamped_row_number_is_never_zero() {
	let lanes = AppendLanes::new(OperatorId(1), 3, [Some(0), Some(1)]);

	assert_eq!(lanes.stamp(0, RowNumber(1)), RowNumber(8));
	assert_ne!(lanes.stamp(0, RowNumber(1)), RowNumber(0));
}

#[test]
#[should_panic(expected = "dropping high bits")]
fn stamping_a_source_row_that_would_lose_high_bits_panics() {
	let lanes = AppendLanes::new(OperatorId(7), 3, [Some(0), Some(1)]);

	lanes.stamp(0, RowNumber(1 << 62));
}

#[test]
fn a_chain_wider_than_the_lane_ceiling_is_rejected() {
	let mut dag = Dag::new();
	let mut left = dag.source(0);
	for id in 1..=MAX_LANES {
		let branch = dag.source(id);
		left = dag.append(1000 + id, left, branch);
	}
	let dag = dag.build();

	assert!(assign_lanes(&dag, left).is_err());
}
