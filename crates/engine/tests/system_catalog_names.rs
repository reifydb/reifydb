// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet};

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::event::EventBus;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::Value};

const RENAMES: [(&str, &str); 15] = [
	("system::queue::queues", "system::queues"),
	("system::flow::flows", "system::flows"),
	("system::enum_variants", "system::enum::variants"),
	("system::event_variants", "system::event::variants"),
	("system::virtual_table_columns", "system::virtual_table::columns"),
	("system::row_shape_fields", "system::row_shape::fields"),
	("system::policy_operations", "system::policy::operations"),
	("system::subscription_watermarks", "system::subscription::watermarks"),
	("system::primary_key_columns", "system::primary_key::columns"),
	("system::column_properties", "system::column::properties"),
	("system::identity_attributes", "system::identity::attributes"),
	("system::identity_attribute_values", "system::identity::attribute_values"),
	("system::granted_roles", "system::identity::roles"),
	("system::windowed_view_group_members", "system::windowed_view_group::members"),
	("system::authentication::credentials", "system::identity::credentials"),
];

const NEW_NAMESPACES: [(&str, u64); 10] = [
	("system::enum", 5),
	("system::event", 6),
	("system::virtual_table", 34),
	("system::row_shape", 35),
	("system::policy", 36),
	("system::subscription", 45),
	("system::primary_key", 48),
	("system::column", 49),
	("system::identity", 50),
	("system::windowed_view_group", 51),
];

fn bootstrapped() -> TestEngine {
	// Without the full bootstrap the system::procedure and system::binding namespaces do not exist.
	let t = TestEngine::new();
	let services = t.inner().services();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	bootstrap_system_objects(
		t.inner().multi(),
		t.inner().single(),
		services.catalog.cache(),
		&eventbus,
		&services.runtime_context,
	)
	.expect("bootstrap must succeed");
	t
}

fn error_code(t: &TestEngine, rql: &str) -> Option<String> {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	result.error.map(|e| e.diagnostic().code.clone())
}

fn strings(t: &TestEngine, rql: &str, column: &str) -> Vec<String> {
	let frames = t.query(rql);
	let col = frames[0].column(column).unwrap().expect("column present");
	(0..col.len()).map(|i| col.get_value(i).to_string()).collect()
}

fn ids_by_kind(t: &TestEngine, rql: &str) -> BTreeMap<String, BTreeSet<String>> {
	let ids = strings(t, rql, "id");
	let kinds = strings(t, rql, "kind");
	let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
	for (id, kind) in ids.into_iter().zip(kinds) {
		assert!(out.entry(kind.clone()).or_default().insert(id.clone()), "{kind} id {id} listed twice");
	}
	out
}

fn function_rows(t: &TestEngine, name: &str) -> Vec<BTreeMap<String, Value>> {
	let frames = t.query(&format!("from system::functions filter {{name == '{name}'}}"));
	let frame = &frames[0];
	let columns = ["name", "namespace", "kind", "min_args", "max_args", "alias_of"];
	let len = frame.column("name").unwrap().unwrap().len();
	(0..len).map(|i| {
		columns.iter().map(|c| (c.to_string(), frame.column(c).unwrap().unwrap().get_value(i))).collect()
	})
	.collect()
}

#[test]
fn every_new_catalog_name_resolves() {
	// A missing compile arm or resolver namespace makes the new name fail with CA_023.
	let t = TestEngine::new();
	for (_, new) in RENAMES {
		assert_eq!(error_code(&t, &format!("from {new}")), None, "{new} must resolve");
	}
	for name in ["system::procedures", "system::bindings", "system::functions"] {
		assert_eq!(error_code(&t, &format!("from {name}")), None, "{name} must resolve");
	}
}

#[test]
fn every_old_catalog_name_is_gone() {
	// Old names must stop working; an alias or a left-over arm would let them resolve.
	let t = TestEngine::new();
	for (old, _) in RENAMES {
		let code = error_code(&t, &format!("from {old}"));
		assert!(matches!(code.as_deref(), Some("CA_023") | Some("CA_002")), "{old} must fail, got {code:?}");
	}
}

#[test]
fn new_system_namespaces_have_their_reserved_ids_and_no_id_is_shared() {
	// A gap id reused by another namespace would make two namespaces collide in a fresh store.
	let t = TestEngine::new();
	let ids = strings(&t, "from system::namespaces", "id");
	let names = strings(&t, "from system::namespaces", "name");
	let unique: BTreeSet<&String> = ids.iter().collect();
	assert_eq!(unique.len(), ids.len(), "namespace ids must be distinct: {ids:?}");
	let by_name: BTreeMap<&str, &str> =
		names.iter().map(String::as_str).zip(ids.iter().map(String::as_str)).collect();
	for (name, id) in NEW_NAMESPACES {
		assert_eq!(by_name.get(name).copied(), Some(id.to_string().as_str()), "{name} must have id {id}");
	}
}

#[test]
fn procedures_list_matches_every_per_kind_table_exactly_once() {
	// A kind missing from the union, or listed twice, breaks parity with the per-kind tables.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE PROCEDURE ns::greet AS { map { message: 'hello' } }");

	let listed = ids_by_kind(&t, "from system::procedures");
	for kind in ["rql", "test", "in_process", "extern_c", "extern_wasm"] {
		let expected: BTreeSet<String> =
			strings(&t, &format!("from system::procedure::{kind}"), "id").into_iter().collect();
		assert_eq!(listed.get(kind).cloned().unwrap_or_default(), expected, "kind {kind}");
	}
	assert_eq!(
		listed.keys()
			.filter(|k| !["rql", "test", "in_process", "extern_c", "extern_wasm"].contains(&k.as_str()))
			.count(),
		0,
		"unknown kind in {listed:?}"
	);
	assert!(listed.get("rql").is_some_and(|ids| !ids.is_empty()), "the created rql procedure must be listed");
	assert!(
		listed.get("in_process").is_some_and(|ids| !ids.is_empty()),
		"builtin in-process procedures must be listed"
	);
}

#[test]
fn bindings_list_matches_every_per_kind_table() {
	// A protocol missing from the list would hide that binding from system::bindings.
	let t = bootstrapped();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE PROCEDURE ns::greet AS { map { message: 'hello' } }");
	t.admin(
		"CREATE HTTP BINDING ns::greet_http FOR ns::greet WITH { method: \"GET\", path: \"/greet\", format: \"json\" }",
	);
	t.admin("CREATE WS BINDING ns::greet_w FOR ns::greet WITH { name: \"greet_w\", format: \"rbcf\" }");

	let listed = ids_by_kind(&t, "from system::bindings");
	for kind in ["http", "grpc", "ws"] {
		let expected: BTreeSet<String> =
			strings(&t, &format!("from system::binding::{kind}"), "id").into_iter().collect();
		assert_eq!(listed.get(kind).cloned().unwrap_or_default(), expected, "kind {kind}");
	}
	assert_eq!(listed.get("http").map(BTreeSet::len), Some(1));
	assert_eq!(listed.get("ws").map(BTreeSet::len), Some(1));
	assert_eq!(strings(&t, "from system::bindings filter {kind == 'http'}", "format"), vec!["json".to_string()]);
}

#[test]
fn functions_list_one_row_per_kind_with_arity_bounds() {
	// math::sum is AtLeast(1) and both scalar and aggregate; mapping AtLeast to max = min or dropping a kind fails
	// here.
	let t = TestEngine::new();
	let rows = function_rows(&t, "math::sum");
	let kinds: BTreeSet<String> = rows.iter().map(|r| r["kind"].to_string()).collect();
	assert_eq!(kinds, BTreeSet::from(["aggregate".to_string(), "scalar".to_string()]));
	for row in &rows {
		assert_eq!(row["namespace"].to_string(), "math");
		assert_eq!(row["min_args"].to_string(), "1");
		assert!(
			matches!(row["max_args"], Value::None { .. }),
			"AtLeast has no upper bound, got {:?}",
			row["max_args"]
		);
		assert!(matches!(row["alias_of"], Value::None { .. }), "math::sum is canonical");
	}

	let age = function_rows(&t, "datetime::age");
	assert_eq!(age.len(), 1);
	assert_eq!(age[0]["min_args"].to_string(), "2");
	assert_eq!(age[0]["max_args"].to_string(), "2");
}

#[test]
fn functions_list_marks_aliases_with_their_canonical_name() {
	// Without the alias record an alias would show up as a plain function with no alias_of.
	let t = TestEngine::new();
	let alias = function_rows(&t, "duration::day");
	assert!(!alias.is_empty(), "alias must be listed");
	for row in &alias {
		assert_eq!(row["alias_of"].to_string(), "duration::days");
	}
	let canonical = function_rows(&t, "duration::days");
	assert!(!canonical.is_empty());
	for row in &canonical {
		assert!(matches!(row["alias_of"], Value::None { .. }), "canonical must not carry alias_of");
	}
	let names = strings(&t, "from system::functions", "name");
	assert!(names.iter().all(|n| !n.starts_with("system::builtin::")), "registry key prefix must not leak");
}
