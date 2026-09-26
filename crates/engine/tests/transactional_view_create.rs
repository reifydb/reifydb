// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::identity::IdentityId};

fn refused(t: &TestEngine, rql: &str) -> (String, String) {
	let r = t.inner().admin_as(IdentityId::system(), rql, Params::None);
	let diagnostic = r.error.expect("the create must be refused").diagnostic();
	(diagnostic.code.to_string(), diagnostic.message.to_string())
}

fn view_count(t: &TestEngine) -> usize {
	TestEngine::row_count(&t.query("from system::views filter {name == 'v'}"))
}

#[test]
fn create_transactional_view_stores_the_view_and_its_flow() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");

	let frames = t.admin("CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::src }");

	let created = frames[0].columns.iter().find(|c| c.name == "created").unwrap().data.get_value(0);
	assert_eq!(created.to_string(), "true");
	let views = t.query("from system::views filter {name == 'v'}");
	let kind = views[0].columns.iter().find(|c| c.name == "kind").unwrap().data.get_value(0);
	assert_eq!(kind.to_string(), "transactional");
	assert_eq!(TestEngine::row_count(&t.query("from system::flows filter {name == 'v'}")), 1);
}

#[test]
fn a_join_is_refused_with_flow_084() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");
	t.admin("CREATE TABLE ns::other { id: int4 }");

	let (code, message) = refused(
		&t,
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::src inner join { FROM ns::other } as o using (id, o.id) }",
	);

	assert_eq!(code, "FLOW_084", "got: {message}");
	assert!(message.contains("join"), "got: {message}");
	assert_eq!(view_count(&t), 0);
}

#[test]
fn a_ring_buffer_source_is_refused_with_flow_084() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE RINGBUFFER ns::rb { id: int4 } WITH { capacity: 10 }");

	let (code, message) = refused(&t, "CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::rb }");

	assert_eq!(code, "FLOW_084", "got: {message}");
	assert!(message.contains("ring buffer source"), "got: {message}");
	assert_eq!(view_count(&t), 0);
}

#[test]
fn ring_buffer_storage_is_refused_with_flow_084() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");

	let (code, message) = refused(
		&t,
		"CREATE TRANSACTIONAL RINGBUFFER VIEW ns::v { id: int4 } WITH { capacity: 10 } AS { FROM ns::src }",
	);

	assert_eq!(code, "FLOW_084", "got: {message}");
	assert!(message.contains("ring buffer storage"), "got: {message}");
	assert_eq!(view_count(&t), 0);
}

#[test]
fn a_row_ttl_is_refused_with_flow_084() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");

	let (code, message) = refused(
		&t,
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4 } WITH { row: { ttl: 1h } } AS { FROM ns::src }",
	);

	assert_eq!(code, "FLOW_084", "got: {message}");
	assert!(message.contains("ttl"), "got: {message}");
	assert_eq!(view_count(&t), 0);
}

#[test]
fn reading_a_deferred_view_is_refused_with_flow_085() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");
	t.admin("CREATE DEFERRED VIEW ns::d { id: int4 } AS { FROM ns::src }");

	let (code, message) = refused(&t, "CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::d }");

	assert_eq!(code, "FLOW_085", "got: {message}");
	assert!(message.contains("deferred view d"), "got: {message}");
	assert_eq!(view_count(&t), 0);
}

#[test]
fn create_transactional_view_with_non_query_body_is_rejected_with_query_005() {
	// A non-query AS body must fail with QUERY_005 before the activation seam, never with SUBS_010.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");

	let r = t.inner().admin_as(
		IdentityId::system(),
		"CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { CREATE TABLE ns::other { id: int4 } }",
		Params::None,
	);
	let diagnostic = r.error.expect("a transactional view whose AS body is DDL must be rejected").diagnostic();
	assert_eq!(diagnostic.code, "QUERY_005", "got: {}", diagnostic.message);
}
