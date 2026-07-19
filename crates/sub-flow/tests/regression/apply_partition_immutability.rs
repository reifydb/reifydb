// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// check_partition_immutability (PART_004) only inspects the literal AST column list of an UPDATE
// statement, and the direct-UPDATE VM instructions (PART_002) only guard a base table/ringbuffer/series
// against changing ITS OWN partition column. Neither has any visibility into a `FlowNodeType::Apply`
// node: an arbitrary, embedder/plugin-supplied operator sitting between a source and a partitioned
// Sink*View can synthesize a `Diff::Update` whose post partition-by values differ from pre, entirely
// independent of which column the driving UPDATE statement touched. Before the fix, the Sink*View
// operators silently "relocated" such a row (delete the old partition key, insert under the new one)
// instead of rejecting it, quietly breaking the "partition columns are immutable" invariant that PART_002
// / PART_004 otherwise advertise. This proves the runtime backstop added to the Sink*View operators
// (`ensure_partition_unchanged` in crates/sub-flow/src/operator/sink/partition.rs) catches this case
// regardless of which upstream node produced the diff.

use reifydb::{WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_sdk::{
	config::Config,
	error::Result as SdkResult,
	operator::{
		OperatorLogic, OperatorMetadata,
		column::operator::OperatorColumn,
		context::OperatorContext,
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
	state::RawStatefulOperator,
};
use reifydb_sub_flow::operator::{
	BoxedOperator,
	native::{NativeBridgedOperator, NativeOperatorAdapter},
};
use reifydb_test_harness::db::TestDb;
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

// `ts` only matters for the series-backed test (SERIES VIEW requires a sequence `key` column); the
// table- and ringbuffer-backed tests carry it along unused so all three share one operator/row shape.
struct FlipRow {
	id: i32,
	ts: i64,
	region: String,
	qty: i32,
}

row!(FlipRow {
	id: i32,
	ts: i64,
	region: String,
	qty: i32
});

const FLIP_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "id",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
		description: "id",
	},
	OperatorColumn {
		name: "ts",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "ts",
	},
	OperatorColumn {
		name: "region",
		type_constraint: TypeConstraint::unconstrained(ValueType::Utf8),
		description: "region",
	},
	OperatorColumn {
		name: "qty",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
		description: "qty",
	},
];

// Passthrough operator that rewrites the `region` column on every UPDATE diff it forwards, regardless of
// what the driving UPDATE statement actually assigned. Stands in for any Apply-node operator that can
// synthesize a partition-changing diff outside check_partition_immutability's literal-AST-column view.
struct RegionFlip;

impl RawStatefulOperator for RegionFlip {}

impl OperatorMetadata for RegionFlip {
	const NAME: &'static str = "region_flip";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str =
		"test-only operator that rewrites the partition column on every forwarded update";
	const INPUT_COLUMNS: &'static [OperatorColumn] = FLIP_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = FLIP_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn read_row(row: &impl RowView) -> FlipRow {
	FlipRow {
		id: row.i32("id").expect("id"),
		ts: row.i64("ts").expect("ts"),
		region: row.utf8("region").expect("region").to_string(),
		qty: row.i32("qty").expect("qty"),
	}
}

impl OperatorLogic for RegionFlip {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(RegionFlip)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		for i in 0..change.diff_count() {
			let Some(diff) = change.diff(i) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(post) = diff.post() {
						let mut rows = Vec::new();
						let mut rns = Vec::new();
						for r in 0..post.row_count() {
							let row = post.row(r).expect("row");
							rns.push(row.row_number().expect("row number"));
							rows.push(read_row(&row));
						}
						ctx.emit_insert(&rows, &rns)?;
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						let mut pre_rows = Vec::new();
						let mut post_rows = Vec::new();
						let mut rns = Vec::new();
						for r in 0..post.row_count() {
							let pre_row = pre.row(r).expect("pre row");
							let post_row = post.row(r).expect("post row");
							rns.push(post_row.row_number().expect("row number"));
							pre_rows.push(read_row(&pre_row));
							let mut flipped = read_row(&post_row);
							flipped.region = if flipped.region == "us" {
								"eu"
							} else {
								"us"
							}
							.to_string();
							post_rows.push(flipped);
						}
						ctx.emit_update(&pre_rows, &post_rows, &rns)?;
					}
				}
				DiffType::Remove => {
					if let Some(pre) = diff.pre() {
						let mut rows = Vec::new();
						let mut rns = Vec::new();
						for r in 0..pre.row_count() {
							let row = pre.row(r).expect("row");
							rns.push(row.row_number().expect("row number"));
							rows.push(read_row(&row));
						}
						ctx.emit_remove(&rows, &rns)?;
					}
				}
			}
		}
		Ok(())
	}
}

fn region_flip(node: FlowNodeId, config: &Config) -> reifydb_value::Result<BoxedOperator> {
	let logic = RegionFlip::create(node, config)?;
	let capabilities = <RegionFlip as OperatorMetadata>::CAPABILITIES;
	let adapter = NativeOperatorAdapter::new(logic, node, capabilities);
	Ok(Box::new(NativeBridgedOperator::new(Box::new(adapter), node, capabilities)))
}

// Drives one instance of the reproduction against a `CREATE VIEW ...` clause supplied by the caller (one
// of Table/RingBuffer/Series-backed storage), asserting the APPLY-operator-driven partition-column change
// is rejected the same way a direct partition-column UPDATE would be.
fn assert_apply_partition_change_rejected(create_view_rql: &str) {
	let db = TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator("region_flip", region_flip))
			.build()
			.expect("build memory db with flow"),
	);
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, ts: int8, region: utf8, qty: int4 }");
	db.admin(create_view_rql);

	db.command("INSERT app::t [{ id: 1, ts: 1, region: \"us\", qty: 10 }]");

	// Only `qty` is assigned here; PART_004's literal-AST check has nothing to flag even though
	// region_flip will rewrite `region` (the view's partition column) on this row's forwarded diff.
	let err = db.try_command("UPDATE app::t { qty: 999 } FILTER id == 1").expect_err(
		"an APPLY operator changing a downstream view's partition column must be rejected, not \
		 silently relocate the row",
	);
	let diagnostic = err.diagnostic();

	assert_eq!(
		diagnostic.code, "PART_002",
		"must fail with the same immutable-partition-column diagnostic as a direct partition-column \
		 UPDATE; got {:?}",
		diagnostic
	);
}

// A table-backed downstream view's partition-by column must stay immutable even when the value change is
// introduced by an APPLY operator rather than a literal `UPDATE ... SET <partition column>`.
#[test]
fn apply_operator_cannot_bypass_partition_column_immutability_table_backed() {
	assert_apply_partition_change_rejected(
		"CREATE VIEW app::v { id: int4, ts: int8, region: utf8, qty: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM app::t APPLY region_flip{} }",
	);
}

// Same reproduction against a ring-buffer-backed downstream view: SinkRingBufferViewOperator's
// partition_changed relocate path is the one this repro was originally filed against.
#[test]
fn apply_operator_cannot_bypass_partition_column_immutability_ringbuffer_backed() {
	assert_apply_partition_change_rejected(
		"CREATE TRANSACTIONAL RINGBUFFER VIEW app::v { id: int4, ts: int8, region: utf8, qty: int4 } \
		 WITH { capacity: 4, partition: { by: { region } } } AS { FROM app::t APPLY region_flip{} }",
	);
}

// Same reproduction against a series-backed downstream view.
#[test]
fn apply_operator_cannot_bypass_partition_column_immutability_series_backed() {
	assert_apply_partition_change_rejected(
		"CREATE TRANSACTIONAL SERIES VIEW app::v { id: int4, ts: int8, region: utf8, qty: int4 } \
		 WITH { key: ts, partition: { by: { region } } } AS { FROM app::t APPLY region_flip{} }",
	);
}
