// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// PART_004 inspects only the literal AST columns of an UPDATE and PART_002 only guards a base object
// against changing its own partition column, so neither can see an APPLY operator synthesizing an
// update whose partition values differ from pre. The sink's runtime check is the only backstop.

use reifydb::{WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::interface::catalog::flow::OperatorId;
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
use reifydb_test_harness::db::TestDb;
use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

// `ts` only matters for the series-backed test, which needs a sequence key; the other two carry it
// unused so all three share one operator and row shape.
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

// Rewrites `region` on every update diff it forwards, whatever the driving statement assigned.
// Stands in for any apply-node operator that can synthesize a partition-changing diff.
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
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
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

fn assert_apply_partition_change_rejected(create_view_rql: &str) {
	// The caller supplies the storage backing, so the same reproduction runs against table, ring
	// buffer and series sinks, each of which has its own relocate path.
	let db = TestDb::from(
		embedded::memory()
			.with_flow(|f| f.register_operator::<RegionFlip>())
			.build()
			.expect("build memory db with flow"),
	);
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, ts: int8, region: utf8, qty: int4 }");
	db.admin(create_view_rql);

	db.command("INSERT app::t [{ id: 1, ts: 1, region: \"us\", qty: 10 }]");

	// Only `qty` is assigned, so the literal-AST check has nothing to flag even though region_flip
	// rewrites the view's partition column on the forwarded diff.
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

#[test]
fn apply_operator_cannot_bypass_partition_column_immutability_table_backed() {
	assert_apply_partition_change_rejected(
		"CREATE VIEW app::v { id: int4, ts: int8, region: utf8, qty: int4 } \
		 WITH { partition: { by: { region } } } AS { FROM app::t APPLY region_flip{} }",
	);
}

#[test]
fn apply_operator_cannot_bypass_partition_column_immutability_ringbuffer_backed() {
	assert_apply_partition_change_rejected(
		"CREATE TRANSACTIONAL RINGBUFFER VIEW app::v { id: int4, ts: int8, region: utf8, qty: int4 } \
		 WITH { capacity: 4, partition: { by: { region } } } AS { FROM app::t APPLY region_flip{} }",
	);
}

#[test]
fn apply_operator_cannot_bypass_partition_column_immutability_series_backed() {
	assert_apply_partition_change_rejected(
		"CREATE TRANSACTIONAL SERIES VIEW app::v { id: int4, ts: int8, region: utf8, qty: int4 } \
		 WITH { key: ts, partition: { by: { region } } } AS { FROM app::t APPLY region_flip{} }",
	);
}
