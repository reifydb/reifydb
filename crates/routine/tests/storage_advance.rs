// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{event::EventBus, value::column::columns::Columns};
use reifydb_routine::procedure::storage::advance::StorageAdvanceProcedure;
use reifydb_routine_abi::{Routine, context::ProcedureContext, error::RoutineError};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, datetime::DateTime, frame::frame::Frame, identity::IdentityId},
};

fn bootstrapped() -> TestEngine {
	let t = TestEngine::new();
	let services = t.inner().services();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	bootstrap_system_objects(t.inner().multi(), t.inner().single(), services.catalog.cache(), &eventbus)
		.expect("bootstrap must succeed");
	t
}

fn asserted(t: &TestEngine) -> Vec<(u64, String)> {
	let frames = t.query("from system::source::completeness");
	let Some(frame) = frames.first() else {
		return vec![];
	};
	let objects = frame.columns.iter().find(|c| c.name.as_str() == "object_id").expect("object_id column");
	let instants =
		frame.columns.iter().find(|c| c.name.as_str() == "complete_through").expect("complete_through column");

	let mut rows: Vec<(u64, String)> = (0..objects.data.len())
		.filter_map(|row| match (objects.data.get_value(row), instants.data.get_value(row)) {
			(Value::Uint8(object), Value::DateTime(at)) => Some((object, at.to_string())),
			_ => None,
		})
		.collect();
	rows.sort();
	rows
}

fn object_id(t: &TestEngine, name: &str) -> u64 {
	let frames = t.query(&format!("from system::tables filter {{ name == '{name}' }}"));
	let frame = frames.first().expect("system::tables frame");
	let ids = frame.columns.iter().find(|c| c.name.as_str() == "id").expect("id column");
	match ids.data.get_value(0) {
		Value::Uint8(id) => id,
		other => panic!("table '{name}' has no numeric id: {other:?}"),
	}
}

fn at(instant: &str) -> String {
	format!("{instant}.000000000Z")
}

fn assert_through(t: &TestEngine, objects: &str, instant: &str) -> Vec<Frame> {
	t.command(&format!("call storage::advance({objects}, cast('{instant}Z', datetime))"))
}

fn assert_through_err(t: &TestEngine, objects: &str, instant: &str) -> String {
	t.command_err(&format!("call storage::advance({objects}, cast('{instant}Z', datetime))"))
}

fn run_directly(t: &TestEngine, identity: IdentityId, args: Vec<Value>) -> Result<Columns, RoutineError> {
	let services = t.inner().services();
	let catalog = services.catalog.clone();
	let params = Params::from(args);
	let mut txn = t.inner().begin_command(identity).expect("command transaction");
	let mut tx = Transaction::Command(&mut txn);
	let mut ctx = ProcedureContext {
		fragment: Fragment::internal("storage::advance"),
		identity,
		row_count: 1,
		runtime_context: &services.runtime_context,
		tx: &mut tx,
		params: &params,
		catalog: &catalog,
		ioc: &services.ioc,
	};
	StorageAdvanceProcedure::new().execute(&mut ctx, &Columns::empty())
}

#[test]
fn an_identifier_argument_resolves_to_the_object_it_names() {
	// Without name parsing the argument is a column reference in a rowless context and evaluates to none.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");
	let trades = object_id(&t, "trades");

	assert_through(&t, "app::trades", "2026-08-07T10:00:00");

	assert_eq!(
		asserted(&t),
		vec![(trades, at("2026-08-07T10:00:00"))],
		"the assertion must land against the id the identifier names"
	);
}

#[test]
fn a_hyphenated_object_name_resolves() {
	// Hyphens lex as Minus, so without hyphen-aware parsing the name stops at its first segment.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::`trade-event` { id: int4 }");
	let event = object_id(&t, "trade-event");

	assert_through(&t, "app::trade-event", "2026-08-07T10:00:00");

	assert_eq!(
		asserted(&t),
		vec![(event, at("2026-08-07T10:00:00"))],
		"a hyphenated name must resolve to the object it spells"
	);
}

#[test]
fn a_list_argument_asserts_every_object_it_names() {
	// A list that compiled to none would arrive as one missing value and silently assert nothing.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");
	t.admin("CREATE TABLE app::quotes { id: int4 }");
	let trades = object_id(&t, "trades");
	let quotes = object_id(&t, "quotes");

	assert_through(&t, "[app::trades, app::quotes]", "2026-08-07T10:00:00");

	let mut expected = vec![(trades, at("2026-08-07T10:00:00")), (quotes, at("2026-08-07T10:00:00"))];
	expected.sort();
	assert_eq!(asserted(&t), expected, "every named object must be asserted, not just the first");
}

#[test]
fn a_namespace_argument_expands_to_its_objects() {
	// Expansion must stop at the namespace boundary; otherwise an unrelated source is advanced too.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");
	t.admin("CREATE TABLE app::quotes { id: int4 }");
	t.admin("CREATE NAMESPACE other");
	t.admin("CREATE TABLE other::untouched { id: int4 }");
	let trades = object_id(&t, "trades");
	let quotes = object_id(&t, "quotes");

	assert_through(&t, "app", "2026-08-07T10:00:00");

	let mut expected = vec![(trades, at("2026-08-07T10:00:00")), (quotes, at("2026-08-07T10:00:00"))];
	expected.sort();
	assert_eq!(asserted(&t), expected, "expansion must cover the namespace's objects and no others");
}

#[test]
fn an_unknown_identifier_fails_the_whole_call() {
	// A partial assertion advances some sources and leaves the rest pinned, hiding the stall it causes.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");

	let err = assert_through_err(&t, "[app::trades, app::typo]", "2026-08-07T10:00:00");

	assert!(err.contains("app::typo"), "the error must name the identifier that failed to resolve: {err}");
	assert_eq!(asserted(&t), vec![], "no object may be asserted when any identifier in the call is unknown");
}

#[test]
fn a_backwards_assertion_is_rejected() {
	// A monotone watermark swallows a regression, so a stale T would never be reported to its writer.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");
	let trades = object_id(&t, "trades");

	assert_through(&t, "app::trades", "2026-08-07T10:00:00");
	let err = assert_through_err(&t, "app::trades", "2026-08-07T09:00:00");

	assert!(err.contains("regression"), "a backwards assertion must be reported as a regression: {err}");
	assert_eq!(
		asserted(&t),
		vec![(trades, at("2026-08-07T10:00:00"))],
		"the rejected assertion must not overwrite the recorded instant"
	);
}

#[test]
fn a_repeated_assertion_updates_in_place() {
	// One row per object is the fold's invariant; a second row makes the object assert twice in a version.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");
	let trades = object_id(&t, "trades");

	assert_through(&t, "app::trades", "2026-08-07T10:00:00");
	assert_through(&t, "app::trades", "2026-08-07T11:00:00");

	assert_eq!(
		asserted(&t),
		vec![(trades, at("2026-08-07T11:00:00"))],
		"a repeated assertion must replace the row rather than append a second one"
	);
}

#[test]
fn an_equal_assertion_is_not_a_regression() {
	// A source re-asserting its current position is idle, not wrong, and must not poison the caller.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");
	let trades = object_id(&t, "trades");

	assert_through(&t, "app::trades", "2026-08-07T10:00:00");
	assert_through(&t, "app::trades", "2026-08-07T10:00:00");

	assert_eq!(
		asserted(&t),
		vec![(trades, at("2026-08-07T10:00:00"))],
		"re-asserting the recorded instant must be accepted and leave one row"
	);
}

#[test]
fn the_ingestor_statement_shape_asserts_every_table_it_lists() {
	// The ingestor binds the instant as a parameter and lists hyphenated names together, a combination inline
	// literals never exercise.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE solana");
	t.admin("CREATE TABLE solana::`pump-trade` { id: int4 }");
	t.admin("CREATE TABLE solana::`raydium-swap` { id: int4 }");
	let pump = object_id(&t, "pump-trade");
	let raydium = object_id(&t, "raydium-swap");

	let instant = DateTime::from_epoch_secs(1_700_000_000).expect("datetime");
	let mut map = HashMap::with_capacity(1);
	map.insert("complete_through".to_string(), Value::DateTime(instant));

	let result = t.inner().command_as(
		IdentityId::system(),
		"call storage::advance([solana::pump-trade, solana::raydium-swap], $complete_through)",
		Params::Named(Arc::new(map)),
	);

	assert!(result.error.is_none(), "the ingestor's statement must execute: {:?}", result.error);
	let mut expected = vec![(pump, instant.to_string()), (raydium, instant.to_string())];
	expected.sort();
	assert_eq!(asserted(&t), expected, "a parameter-bound instant must land on every table in the list");
}

#[test]
fn an_unprivileged_identity_cannot_assert() {
	// Authority must come from the identity, never from whatever policy is granted on the procedure.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");

	let instant = DateTime::from_ymd_hms(2026, 8, 7, 10, 0, 0).expect("datetime");
	let result = run_directly(
		&t,
		IdentityId::anonymous(),
		vec![Value::Utf8("app::trades".to_string()), Value::DateTime(instant)],
	);

	let err = result.expect_err("an anonymous identity must not be able to assert completeness");
	assert!(
		format!("{err}").contains("privileged"),
		"the refusal must name the missing privilege rather than fail deep inside the write: {err}"
	);
	assert_eq!(asserted(&t), vec![], "a refused call must write nothing");
}

#[test]
fn a_privileged_identity_asserts_through_the_same_path() {
	// The guard must gate the caller and not the procedure, or no ingestor could ever assert.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::trades { id: int4 }");
	let trades = object_id(&t, "trades");

	let instant = DateTime::from_ymd_hms(2026, 8, 7, 10, 0, 0).expect("datetime");
	let columns = run_directly(
		&t,
		IdentityId::root(),
		vec![Value::Utf8("app::trades".to_string()), Value::DateTime(instant)],
	)
	.expect("a privileged identity must be able to assert completeness");

	let ids = columns.iter().find(|c| c.name().text() == "object_id").expect("object_id column");
	assert_eq!(ids.data().get_value(0), Value::Uint8(trades), "the returned frame must name the asserted object");
}
