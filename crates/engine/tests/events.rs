// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;

#[test]
fn test_create_event_basic() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	let frames = t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");

	let frame = &frames[0];
	assert_eq!(frame.get::<String>("namespace", 0).unwrap().unwrap(), "ns");
	assert_eq!(frame.get::<String>("event", 0).unwrap().unwrap(), "order_event");
	assert_eq!(frame.get::<bool>("created", 0).unwrap().unwrap(), true);
}

#[test]
fn test_create_event_multiple_variants() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	let frames = t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 }, OrderShipped { id: int4 } }");

	let frame = &frames[0];
	assert_eq!(frame.get::<String>("event", 0).unwrap().unwrap(), "order_event");
	assert_eq!(frame.get::<bool>("created", 0).unwrap().unwrap(), true);
}

#[test]
fn test_create_enum_is_not_event_handler_rejected() {
	// CREATE HANDLER on a plain ENUM must fail at compile (physical planning) time.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE ENUM ns::status { Active, Inactive }");

	let msg = t.admin_err("CREATE HANDLER ns::h ON ns::status::Active { }");
	// Physical planner returns: "'status' is not an EVENT type. Use CREATE EVENT..."
	assert!(
		msg.to_lowercase().contains("not an event") || msg.to_lowercase().contains("event type"),
		"Expected 'not an event' error, got: {msg}"
	);
}

#[test]
fn test_create_handler_basic() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");

	let frames = t.admin("CREATE HANDLER ns::on_placed ON ns::order_event::OrderPlaced \
		 { INSERT ns::audit [{ kind: \"placed\" }] }");

	let frame = &frames[0];
	assert_eq!(frame.get::<String>("namespace", 0).unwrap().unwrap(), "ns");
	assert_eq!(frame.get::<String>("handler", 0).unwrap().unwrap(), "on_placed");
	assert_eq!(frame.get::<bool>("created", 0).unwrap().unwrap(), true);
}

#[test]
fn test_create_handler_unknown_variant() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");

	let msg = t.admin_err("CREATE HANDLER ns::h ON ns::order_event::NonExistent { }");
	assert!(
		msg.contains("NonExistent")
			|| msg.to_lowercase().contains("variant")
			|| msg.to_lowercase().contains("not found"),
		"Expected variant-not-found error, got: {msg}"
	);
}

#[test]
fn test_dispatch_no_handlers() {
	// Dispatching an event with no registered handlers is a no-op - zero handlers fired.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");

	let frames = t.command("DISPATCH ns::order_event::OrderPlaced { id: 1 }");
	let fired: u8 = frames[0].get::<u8>("handlers_fired", 0).unwrap().unwrap();
	assert_eq!(fired, 0);
}

#[test]
fn test_dispatch_single_handler() {
	// One handler fires and produces a side-effect row in the audit table.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");
	t.admin("CREATE HANDLER ns::on_placed ON ns::order_event::OrderPlaced \
		 { INSERT ns::audit [{ kind: \"placed\" }] }");

	let frames = t.command("DISPATCH ns::order_event::OrderPlaced { id: 42 }");
	let fired: u8 = frames[0].get::<u8>("handlers_fired", 0).unwrap().unwrap();
	assert_eq!(fired, 1);

	// Verify the INSERT side-effect
	let audit = t.query("FROM ns::audit");
	assert_eq!(TestEngine::row_count(&audit), 1);
	let kind: String = audit[0].get::<String>("kind", 0).unwrap().unwrap();
	assert_eq!(kind, "placed");
}

#[test]
fn test_dispatch_fanout_two_handlers() {
	// Two handlers registered on the same variant - both must fire.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");
	t.admin("CREATE HANDLER ns::handler_a ON ns::order_event::OrderPlaced \
		 { INSERT ns::audit [{ kind: \"a\" }] }");
	t.admin("CREATE HANDLER ns::handler_b ON ns::order_event::OrderPlaced \
		 { INSERT ns::audit [{ kind: \"b\" }] }");

	let frames = t.command("DISPATCH ns::order_event::OrderPlaced { id: 1 }");
	let fired: u8 = frames[0].get::<u8>("handlers_fired", 0).unwrap().unwrap();
	assert_eq!(fired, 2);

	let audit = t.query("FROM ns::audit");
	assert_eq!(TestEngine::row_count(&audit), 2);
}

#[test]
fn test_dispatch_only_matching_variant() {
	// A handler registered on variant B must NOT fire when variant A is dispatched.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 }, OrderShipped { id: int4 } }");
	t.admin("CREATE HANDLER ns::on_shipped ON ns::order_event::OrderShipped \
		 { INSERT ns::audit [{ kind: \"shipped\" }] }");

	// Dispatch OrderPlaced - the handler is for OrderShipped, should not fire.
	let frames = t.command("DISPATCH ns::order_event::OrderPlaced { id: 1 }");
	let fired: u8 = frames[0].get::<u8>("handlers_fired", 0).unwrap().unwrap();
	assert_eq!(fired, 0);

	let audit = t.query("FROM ns::audit");
	assert_eq!(TestEngine::row_count(&audit), 0);
}

#[test]
fn test_dispatch_chained_events() {
	// Handler A DISPATCHes event B; handler B inserts a row - both effects land in the
	// same transaction.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 }, OrderShipped { id: int4 } }");

	// handler_a fires on OrderPlaced, dispatches OrderShipped
	t.admin("CREATE HANDLER ns::on_placed ON ns::order_event::OrderPlaced \
		 { INSERT ns::audit [{ kind: \"placed\" }]; DISPATCH ns::order_event::OrderShipped { id: 1 } }");

	// handler_b fires on OrderShipped
	t.admin("CREATE HANDLER ns::on_shipped ON ns::order_event::OrderShipped \
		 { INSERT ns::audit [{ kind: \"shipped\" }] }");

	t.command("DISPATCH ns::order_event::OrderPlaced { id: 1 }");

	// Both "placed" and "shipped" rows should exist in the same committed transaction.
	let audit = t.query("FROM ns::audit");
	assert_eq!(TestEngine::row_count(&audit), 2);

	let mut kinds: Vec<String> = (0..2).map(|i| audit[0].get::<String>("kind", i).unwrap().unwrap()).collect();
	kinds.sort();
	assert_eq!(kinds, vec!["placed", "shipped"]);
}

#[test]
fn test_dispatch_handler_accesses_event_fields() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { order_id: int4, note: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4, note: utf8 } }");
	t.admin("CREATE HANDLER ns::on_placed ON ns::order_event::OrderPlaced \
		 { INSERT ns::audit [{ order_id: event_id, note: event_note }] }");

	t.command("DISPATCH ns::order_event::OrderPlaced { id: 42, note: \"express\" }");

	let frames = t.query("FROM ns::audit");
	let frame = &frames[0];
	assert_eq!(frame.get::<i32>("order_id", 0).unwrap().unwrap(), 42);
	assert_eq!(frame.get::<String>("note", 0).unwrap().unwrap(), "express");
}

#[test]
fn test_dispatch_wrong_type_enum_not_event() {
	// Dispatching to a plain ENUM must fail (physical planner catches at compile time).
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE ENUM ns::status { Active, Inactive }");

	// DISPATCH targets a SumType by name; physical planner checks is_event.
	let msg = t.command_err("DISPATCH ns::status::Active { }");
	assert!(
		msg.to_lowercase().contains("not an event") || msg.to_lowercase().contains("event type"),
		"Expected event-type error, got: {msg}"
	);
}
