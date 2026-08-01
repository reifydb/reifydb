// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;

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
	// A handler on a plain ENUM must fault at plan time; letting it register would create a
	// handler nothing can ever dispatch to.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE ENUM ns::status { Active, Inactive }");

	let msg = t.admin_err("CREATE HANDLER ns::h ON ns::status::Active { }");
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
	// An unhandled dispatch is a no-op, not a fault.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");

	let frames = t.command("DISPATCH ns::order_event::OrderPlaced { id: 1 }");
	let fired: u8 = frames[0].get::<u8>("handlers_fired", 0).unwrap().unwrap();
	assert_eq!(fired, 0);
}

#[test]
fn test_dispatch_single_handler() {
	// The reported count must be backed by a real side effect, not just a registration lookup.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 } }");
	t.admin("CREATE HANDLER ns::on_placed ON ns::order_event::OrderPlaced \
		 { INSERT ns::audit [{ kind: \"placed\" }] }");

	let frames = t.command("DISPATCH ns::order_event::OrderPlaced { id: 42 }");
	let fired: u8 = frames[0].get::<u8>("handlers_fired", 0).unwrap().unwrap();
	assert_eq!(fired, 1);

	let audit = t.query("FROM ns::audit");
	assert_eq!(TestEngine::row_count(&audit), 1);
	let kind: String = audit[0].get::<String>("kind", 0).unwrap().unwrap();
	assert_eq!(kind, "placed");
}

#[test]
fn test_dispatch_fanout_two_handlers() {
	// Dispatch fans out; stopping after the first handler would silently drop the rest.
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
	// Handlers are keyed by variant, not by event type; keying by type would fire all of them.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 }, OrderShipped { id: int4 } }");
	t.admin("CREATE HANDLER ns::on_shipped ON ns::order_event::OrderShipped \
		 { INSERT ns::audit [{ kind: \"shipped\" }] }");

	let frames = t.command("DISPATCH ns::order_event::OrderPlaced { id: 1 }");
	let fired: u8 = frames[0].get::<u8>("handlers_fired", 0).unwrap().unwrap();
	assert_eq!(fired, 0);

	let audit = t.query("FROM ns::audit");
	assert_eq!(TestEngine::row_count(&audit), 0);
}

#[test]
fn test_dispatch_chained_events() {
	// A dispatch raised from inside a handler must be delivered within the same transaction,
	// or the second effect would be lost on rollback of the first.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::audit { kind: utf8 }");
	t.admin("CREATE EVENT ns::order_event { OrderPlaced { id: int4 }, OrderShipped { id: int4 } }");

	t.admin("CREATE HANDLER ns::on_placed ON ns::order_event::OrderPlaced \
		 { INSERT ns::audit [{ kind: \"placed\" }]; DISPATCH ns::order_event::OrderShipped { id: 1 } }");

	t.admin("CREATE HANDLER ns::on_shipped ON ns::order_event::OrderShipped \
		 { INSERT ns::audit [{ kind: \"shipped\" }] }");

	t.command("DISPATCH ns::order_event::OrderPlaced { id: 1 }");

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
	// DISPATCH names a sum type, so nothing in the syntax distinguishes an enum from an event;
	// the check has to happen at plan time.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE ENUM ns::status { Active, Inactive }");

	let msg = t.command_err("DISPATCH ns::status::Active { }");
	assert!(
		msg.to_lowercase().contains("not an event") || msg.to_lowercase().contains("event type"),
		"Expected event-type error, got: {msg}"
	);
}
