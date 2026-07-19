// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Reproducer: the deferred (CDC-driven) flow path must classify an UPDATE as DiffType::Update,
// independent of the storage tier configuration, and a transactional view and a deferred view built
// on the same kind-sensitive operator must agree.
//
// WHY this matters: a transactional view and a deferred view built on the same source, fed the same
// DML, must deliver the same per-diff kinds. The transactional inline path authors the diff from the
// executed statement (an UPDATE becomes Diff::Update). The deferred path goes through CDC, which on
// the broken code re-derived the kind by probing the store for the row's previous version
// (`get_previous_version`, "Update iff a prior version exists, else Insert").
//
// That probe is unreliable: the `sqlite_without_buffer` tier keeps only the latest version per key
// (the persistent get rejects any stored version newer than the requested one and there is no
// commit/read buffer holding history), so the prior row is never found and every UPDATE is mislabeled
// as an Insert. Memory and buffered tiers happen to retain the prior version in-buffer and mask the
// defect; the unbuffered tier exposes it. A kind-sensitive operator then tallies updates as inserts on
// the deferred path while the transactional path counts them correctly.
//
// These assertions pin the contract (UPDATE -> Update on every flow path) so they break if the
// classification regresses, not merely if the output shape changes.

use reifydb::{SqliteConfig, WithSubsystem, embedded};
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_sdk::{
	config::Config,
	error::Result as SdkResult,
	operator::{
		OperatorLogic, OperatorMetadata,
		column::operator::OperatorColumn,
		context::OperatorContext,
		view::{ChangeView, ColumnsView, DiffView},
	},
	row,
	state::{RawStatefulOperator, single::SingleStateful},
};
use reifydb_sub_flow::operator::{
	BoxedOperator,
	native::{NativeBridgedOperator, NativeOperatorAdapter},
};
use reifydb_test_harness::db::{TestDb, poll_until};
use reifydb_value::value::{
	Value, constraint::TypeConstraint, duration::Duration, row_number::RowNumber, value_type::ValueType,
};

use crate::common::{drain_after_consumer_caught_up, extract_sub_id};

const OP_INSERT: u8 = 1;
const OP_UPDATE: u8 = 2;

// Stateful operator that tallies the diff kinds it receives into one (insert, update, delete) row.
// It is the minimal kind-sensitive sink that surfaces the deferred-vs-transactional divergence: an
// UPDATE delivered as Insert lands in the wrong tally. Mirrors the testbed `seq_counter` fixture but
// is registered in-process so the test needs no compiled cdylib.
struct KindCounter;

impl RawStatefulOperator for KindCounter {}

impl SingleStateful for KindCounter {
	type State = (i64, i64, i64);
}

struct CounterRow {
	insert: i64,
	update: i64,
	delete: i64,
}

row!(CounterRow {
	insert: i64,
	update: i64,
	delete: i64
});

const COUNTER_OUTPUT_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "insert",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "Cumulative insert diff count",
	},
	OperatorColumn {
		name: "update",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "Cumulative update diff count",
	},
	OperatorColumn {
		name: "delete",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "Cumulative delete diff count",
	},
];

impl OperatorMetadata for KindCounter {
	const NAME: &'static str = "kind_counter";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Running insert/update/delete diff tally in one state row";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = COUNTER_OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for KindCounter {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(KindCounter)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		let mut inserts = 0i64;
		let mut updates = 0i64;
		let mut deletes = 0i64;
		for i in 0..change.diff_count() {
			let Some(diff) = change.diff(i) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => inserts += diff.post().map(|c| c.row_count()).unwrap_or(0) as i64,
				DiffType::Update => updates += diff.post().map(|c| c.row_count()).unwrap_or(0) as i64,
				DiffType::Remove => deletes += diff.pre().map(|c| c.row_count()).unwrap_or(0) as i64,
			}
		}

		let (ins, upd, del) = self.load_state(ctx)?.unwrap_or((0, 0, 0));
		let state = (ins + inserts, upd + updates, del + deletes);
		self.save_state(ctx, &state)?;

		ctx.emit_insert(
			&[CounterRow {
				insert: state.0,
				update: state.1,
				delete: state.2,
			}],
			&[RowNumber(1)],
		)
	}
}

fn kind_counter(node: FlowNodeId, config: &Config) -> reifydb_value::Result<BoxedOperator> {
	let logic = KindCounter::create(node, config)?;
	let capabilities = <KindCounter as OperatorMetadata>::CAPABILITIES;
	let adapter = NativeOperatorAdapter::new(logic, node, capabilities);
	Ok(Box::new(NativeBridgedOperator::new(Box::new(adapter), node, capabilities)))
}

fn make_unbuffered_db() -> TestDb {
	let (config, _guard) = SqliteConfig::in_memory();
	let db = TestDb::from(
		embedded::sqlite_without_buffer(config)
			.with_flow(|f| f.register_operator("kind_counter", kind_counter))
			.build()
			.expect("build unbuffered sqlite db"),
	);
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, qty: int4, ts_ms: int8 }");
	db
}

// In-order `_op` sequence (Insert=1, Update=2, Remove=3) of every delivered row whose `id` matches.
fn ops_for_id(batches: &[reifydb_core::value::column::columns::Columns], target_id: i32) -> Vec<u8> {
	let mut ops = Vec::new();
	for cols in batches {
		let id_col = cols.iter().find(|c| c.name().text() == "id").expect("id column in sink output");
		let op_col = cols.iter().find(|c| c.name().text() == "_op").expect("_op column in sink output");
		for i in 0..cols.row_count() {
			let id = match id_col.data().get_value(i) {
				Value::Int4(v) => v,
				other => panic!("expected Int4 id, got {:?}", other),
			};
			if id != target_id {
				continue;
			}
			let op = match op_col.data().get_value(i) {
				Value::Uint1(v) => v,
				other => panic!("expected Uint1 _op, got {:?}", other),
			};
			ops.push(op);
		}
	}
	ops
}

// The single (insert, update, delete) tally row of a kind_counter view, or None if not yet materialized.
fn read_counts(db: &TestDb, query: &str) -> Option<(i64, i64, i64)> {
	let frames = db.try_query(query).ok()?;
	let frame = frames.first()?;
	let get = |name: &str| -> Option<i64> {
		let col = frame.columns.iter().find(|c| c.name == name)?;
		if col.data.is_empty() {
			return None;
		}
		match col.data.get_value(0) {
			Value::Int8(v) => Some(v),
			_ => None,
		}
	};
	Some((get("insert")?, get("update")?, get("delete")?))
}

// Poll a deferred view until its tally reflects `expected_total` mutations or the deadline passes,
// then return whatever it holds so the caller's assertion reports the actual (possibly wrong) tally.
fn await_counts(db: &TestDb, query: &str, expected_total: i64) -> (i64, i64, i64) {
	poll_until(
		|| read_counts(db, query).filter(|c| c.0 + c.1 + c.2 >= expected_total),
		Duration::from_seconds(10).unwrap().to_std(),
	)
	.unwrap_or_else(|| read_counts(db, query).unwrap_or((-1, -1, -1)))
}

fn insert_then_update(db: &TestDb) {
	db.command("INSERT app::t [{ id: 1, qty: 10, ts_ms: 0 }]");
	db.command("UPDATE app::t { qty: 999 } FILTER id == 1");
}

#[test]
fn deferred_subscription_update_is_classified_as_update_not_insert() {
	let db = make_unbuffered_db();

	let frames = db.admin("CREATE SUBSCRIPTION AS { from app::t | map { id, qty } }");
	let sub_id = extract_sub_id(&frames);

	insert_then_update(&db);

	let batches = drain_after_consumer_caught_up(&db, sub_id);
	let ops = ops_for_id(&batches, 1);

	assert_eq!(
		ops,
		vec![OP_INSERT, OP_UPDATE],
		"deferred/CDC path must deliver the INSERT then the UPDATE as Insert(1) then Update(2); \
		 got {:?} (an update mislabeled as Insert means the CDC path re-derived the diff kind from a \
		 storage previous-version probe instead of the engine's authored classification)",
		ops
	);
}

#[test]
fn transactional_view_counts_update_as_update() {
	let db = make_unbuffered_db();
	db.admin(
		"CREATE VIEW app::tx_counts { insert: int8, update: int8, delete: int8 } AS { FROM app::t APPLY kind_counter{} }",
	);

	insert_then_update(&db);

	// A transactional view materializes inline with the commit, so it is exact immediately.
	let counts = read_counts(&db, "FROM app::tx_counts").expect("transactional view has a tally row");
	assert_eq!(
		counts,
		(1, 1, 0),
		"transactional view must tally exactly one insert and one update; got (insert, update, delete) = {:?}",
		counts
	);
}

#[test]
fn deferred_view_counts_update_as_update_like_transactional() {
	let db = make_unbuffered_db();
	db.admin(
		"CREATE DEFERRED VIEW app::def_counts { insert: int8, update: int8, delete: int8 } AS { FROM app::t APPLY kind_counter{} }",
	);

	insert_then_update(&db);

	// The deferred view catches up via CDC; it must reach the SAME tally as the transactional view.
	let counts = await_counts(&db, "FROM app::def_counts", 2);
	assert_eq!(
		counts,
		(1, 1, 0),
		"deferred view must converge to the same tally as the transactional view (one insert, one \
		 update); got (insert, update, delete) = {:?} (update folded into insert means the CDC path \
		 mislabeled the UPDATE)",
		counts
	);
}
