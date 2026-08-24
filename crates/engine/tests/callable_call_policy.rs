// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{event::EventBus, execution::ExecutionResult};
use reifydb_test_harness::{engine::TestEngine, fixture::identity::identity};
use reifydb_value::{error::Diagnostic, params::Params, value::identity::IdentityId};

const NO_POLICY: &str = "POLICY_002";
const POLICY_DENIED: &str = "POLICY_001";

fn plain() -> (TestEngine, IdentityId) {
	// Without a bootstrap every builtin resolves through the registry, which is the ungated path.
	let t = TestEngine::new();
	let alice = identity("alice").create(&t);
	(t, alice.id)
}

fn bootstrapped() -> (TestEngine, IdentityId) {
	// Without this the seed call policies never exist and the open/closed split cannot be observed.
	let t = TestEngine::new();
	bootstrap(&t);
	let alice = identity("alice").create(&t);
	(t, alice.id)
}

fn bootstrap(t: &TestEngine) {
	let services = t.inner().services();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	bootstrap_system_objects(t.inner().multi(), t.inner().single(), services.catalog.cache(), &eventbus)
		.expect("bootstrap must succeed");
}

fn seed_policy_count(t: &TestEngine, name: &str) -> usize {
	TestEngine::row_count(&t.query(&format!("FROM system::policies FILTER {{ name == '{name}' }}")))
}

fn diagnostic(result: ExecutionResult, what: &str) -> Diagnostic {
	result.error.unwrap_or_else(|| panic!("{what} must have been denied but succeeded")).diagnostic()
}

fn code(result: ExecutionResult, what: &str) -> String {
	diagnostic(result, what).code
}

fn clock_nanos(t: &TestEngine) -> u64 {
	t.mock_clock().now().to_nanos()
}

#[test]
fn a_builtin_procedure_is_denied_when_no_call_policy_exists() {
	// Site 2: clock::advance is registry-only, so deny-by-default must come from the funnel itself.
	let (t, alice) = plain();
	let before = clock_nanos(&t);

	let r = t.inner().command_as(alice, "CALL clock::advance(5000)", Params::None);

	assert_eq!(code(r, "clock::advance"), NO_POLICY);
	assert_eq!(clock_nanos(&t), before, "the routine ran despite the denial");
}

#[test]
fn a_builtin_procedure_is_allowed_by_a_permissive_call_policy() {
	// The gate must read the policy rather than hardcode a verdict.
	let (t, alice) = plain();
	t.admin("CREATE PROCEDURE POLICY clock_advance_open ON clock { call: { filter { true } } }");
	let before = clock_nanos(&t);

	let r = t.inner().command_as(alice, "CALL clock::advance(5000)", Params::None);

	assert!(r.error.is_none(), "a permissive call policy must admit the call: {:?}", r.error);
	assert_eq!(clock_nanos(&t), before + 5_000_000_000, "the routine did not run");
}

#[test]
fn a_denying_call_policy_blocks_the_builtin() {
	// Without this the gate could admit anything that merely has a policy attached.
	let (t, alice) = plain();
	t.admin("CREATE PROCEDURE POLICY clock_advance_shut ON clock { call: { filter { false } } }");
	let before = clock_nanos(&t);

	let r = t.inner().command_as(alice, "CALL clock::advance(5000)", Params::None);

	assert_eq!(code(r, "clock::advance"), POLICY_DENIED);
	assert_eq!(clock_nanos(&t), before, "a denied call still ran the routine");
}

#[test]
fn a_privileged_caller_is_unaffected_by_the_call_gate() {
	// Without the is_privileged short-circuit every internal system-identity call starts failing.
	let (t, _alice) = plain();
	let before = clock_nanos(&t);

	let r = t.inner().command_as(IdentityId::system(), "CALL clock::advance(5000)", Params::None);

	assert!(r.error.is_none(), "a privileged caller must not need a call policy: {:?}", r.error);
	assert_eq!(clock_nanos(&t), before + 5_000_000_000);
}

#[test]
fn a_catalog_procedure_call_is_denied_when_no_call_policy_exists() {
	// Site 1 already gated before the funnel refactor and must keep behaving exactly the same.
	let (t, alice) = plain();
	t.admin("CREATE NAMESPACE cg");
	t.admin("CREATE PROCEDURE cg::greet AS { 'hello' }");

	let r = t.inner().command_as(alice, "CALL cg::greet()", Params::None);

	assert_eq!(code(r, "cg::greet"), NO_POLICY);
}

#[test]
fn a_catalog_procedure_call_is_allowed_by_a_permissive_call_policy() {
	// A policy written against the catalog name must admit the call, exactly as for a builtin.
	let (t, alice) = plain();
	t.admin("CREATE NAMESPACE cg2");
	t.admin("CREATE PROCEDURE cg2::greet AS { 'hello' }");
	t.admin("CREATE PROCEDURE POLICY cg2_greet_open ON cg2::greet { call: { filter { true } } }");

	let r = t.inner().command_as(alice, "CALL cg2::greet()", Params::None);

	assert!(r.error.is_none(), "a permissive call policy must admit the catalog procedure: {:?}", r.error);
}

#[test]
fn a_generator_procedure_is_denied_when_no_call_policy_exists() {
	// Site 4: a FROM source bypasses the CALL opcode entirely and must not escape the gate.
	let (t, alice) = plain();

	let r = t.inner().query_as(alice, "FROM rql::tokenize { 'FROM system::namespaces' }", Params::None);

	assert_eq!(code(r, "rql::tokenize as a generator"), NO_POLICY);
}

#[test]
fn a_generator_procedure_is_allowed_by_a_permissive_call_policy() {
	// Proves the generator site reads the policy rather than failing closed unconditionally.
	let (t, alice) = plain();
	t.admin("CREATE PROCEDURE POLICY rql_tokenize_open ON rql { call: { filter { true } } }");

	let r = t.inner().query_as(alice, "FROM rql::tokenize { 'FROM system::namespaces' }", Params::None);

	assert!(r.error.is_none(), "a permissive call policy must admit the generator: {:?}", r.error);
}

#[test]
fn seeded_open_builtins_are_callable_on_a_fresh_database() {
	// A missing or misnamed seed policy shows up here and nowhere else.
	let (t, alice) = bootstrapped();
	t.admin("CREATE TABLE gq { id: int4 }");

	for rql in [
		"CALL rql::tokenize('FROM system::namespaces')",
		"CALL rql::ast('FROM system::namespaces')",
		"CALL rql::logical('FROM system::namespaces')",
		"CALL rql::explain('FROM system::namespaces')",
		"CALL graphql::explain('query { gq { id } }')",
	] {
		let r = t.inner().command_as(alice, rql, Params::None);
		assert!(r.error.is_none(), "{rql} must be open on a fresh database: {:?}", r.error);
	}
}

#[test]
fn closed_builtins_are_denied_on_a_fresh_database() {
	// The bootstrap must never widen the gate past the five introspection procedures.
	let (t, alice) = bootstrapped();

	for rql in [
		"CALL clock::set(1000)",
		"CALL clock::advance(1000)",
		"CALL storage::advance(1, cast('2026-01-01T00:00:00Z', datetime))",
		"CALL system::config::set('threads.task', 4)",
		"CALL identity::set_attribute('alice', 'org', 'acme')",
		"CALL identity::remove_attribute('alice', 'org')",
		"CALL subscription::inspect(1)",
		"CALL testing::events::dispatched()",
		"CALL testing::handlers::invoked()",
		"CALL testing::tables::changed()",
		"CALL testing::views::changed()",
		"CALL testing::series::changed()",
		"CALL testing::ringbuffers::changed()",
		"CALL testing::dictionaries::changed()",
	] {
		let r = t.inner().command_as(alice, rql, Params::None);
		assert_eq!(code(r, rql), NO_POLICY, "{rql} must stay closed on a fresh database");
	}
}

#[test]
fn identity_inject_is_denied_before_it_can_reach_its_own_guard() {
	// The call must fail on policy, so dropping the routine's own transaction check cannot reopen the hole.
	let (t, alice) = plain();

	let r = t.inner().command_as(
		alice,
		&format!("CALL identity::inject(cast('{}', identity_id))", IdentityId::root()),
		Params::None,
	);

	assert_eq!(code(r, "identity::inject"), NO_POLICY);
}

#[test]
fn a_second_start_does_not_duplicate_the_seed_policies() {
	// A bootstrap that re-creates on every start would accumulate one policy row per restart.
	let t = TestEngine::new();
	bootstrap(&t);
	bootstrap(&t);

	assert_eq!(seed_policy_count(&t, "system_call_rql_explain"), 1, "the seed policy was created twice");
}

#[test]
fn a_dropped_seed_policy_comes_back_on_the_next_start() {
	// The seeds are a floor, not an initial state: dropping one is undone by the next start.
	let t = TestEngine::new();
	bootstrap(&t);
	assert_eq!(seed_policy_count(&t, "system_call_rql_explain"), 1);

	t.admin("DROP PROCEDURE POLICY system_call_rql_explain");
	assert_eq!(seed_policy_count(&t, "system_call_rql_explain"), 0, "the drop did not take effect");

	bootstrap(&t);

	assert_eq!(seed_policy_count(&t, "system_call_rql_explain"), 1, "a dropped seed policy stayed dropped");
}

fn event_engine() -> (TestEngine, IdentityId) {
	// A handler is fired as a consequence of DISPATCH, so the caller never names it and cannot see it.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");
	t.admin(
		"CREATE HANDLER ns::on_placed ON ns::order_event::OrderPlaced { INSERT ns::audit [{ kind: 'placed' }] }",
	);
	let alice = identity("alice").create(&t);
	(t, alice.id)
}

#[test]
fn a_dispatched_event_handler_is_denied_when_no_call_policy_exists() {
	// Site 3: a handler runs with the dispatching identity, so an ungated DISPATCH is an ungated call.
	let (t, alice) = event_engine();

	let r = t.inner().command_as(alice, "DISPATCH ns::order_event::OrderPlaced { id: 1 }", Params::None);
	let d = diagnostic(r, "DISPATCH ns::order_event::OrderPlaced");

	assert_eq!(d.code, "POLICY_003");
	assert_eq!(
		d.message,
		"DISPATCH of order_event::OrderPlaced was refused: no call policy grants call on the handler ns::on_placed"
	);
	assert_eq!(
		d.help.as_deref(),
		Some(
			"A handler runs with the identity that dispatched the event. Grant it with:\n  CREATE PROCEDURE POLICY ON ns::on_placed { call: { filter { ... } } }"
		)
	);
	assert_eq!(TestEngine::row_count(&t.query("FROM ns::audit")), 0, "the handler body ran despite the denial");
}

#[test]
fn a_dispatched_event_handler_is_allowed_by_a_permissive_call_policy() {
	// Proves the dispatch gate reads the policy, and that the handler body still runs once it is granted.
	let (t, alice) = event_engine();
	t.admin("CREATE PROCEDURE POLICY ns_on_placed_open ON ns::on_placed { call: { filter { true } } }");
	t.admin("CREATE TABLE POLICY ns_audit_open ON ns::audit { insert: { filter { true } } }");

	let r = t.inner().command_as(alice, "DISPATCH ns::order_event::OrderPlaced { id: 1 }", Params::None);

	assert!(r.error.is_none(), "a permissive call policy must admit the handler: {:?}", r.error);
	assert_eq!(TestEngine::row_count(&t.query("FROM ns::audit")), 1, "the handler body did not run");
}
