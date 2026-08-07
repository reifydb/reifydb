// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;
use reifydb_test_harness::{
	assert::FrameAssert,
	fixture::{identity::identity, table, view},
	lookup::{find_identity_by_attribute, identity_attribute},
};
use reifydb_value::value::{Value, value_type::ValueType};

#[test]
fn table_builder_creates_a_working_table() {
	let db = TestDb::memory();

	// The builder must create both the namespace (auto-ensured) and the typed columns,
	// so a subsequent insert + query round-trips through the real catalog.
	table("shop::orders").column("id", ValueType::Int4).column("total", ValueType::Float8).create(&db);

	db.command("insert shop::orders [{ id: 1, total: 9.99 }]");
	db.query("from shop::orders").assert().row_count(1);
}

#[test]
fn view_builder_creates_a_view_in_an_auto_ensured_namespace() {
	let db = TestDb::memory();

	// A deferred view is not queryable until a flow materializes it, so the fixture can only
	// promise the catalog object exists under the requested name.
	let view = view("analytics::totals").column("total", ValueType::Float8).create(&db);
	assert_eq!(view.name(), "totals");
}

#[test]
fn identity_builder_writes_lookup_and_display_attributes() {
	let db = TestDb::memory();

	// The builder must write what the auth service would: a github_user_id lookup attribute
	// the identity is resolvable by, alongside any display attributes.
	let alice = identity("alice")
		.attribute("nickname", Value::Utf8("ally".to_string()))
		.github_user(42, "octocat")
		.create(&db);

	assert_eq!(identity_attribute(&db, alice.id, "nickname"), Some(Value::Utf8("ally".to_string())));
	assert_eq!(identity_attribute(&db, alice.id, "github_user_id"), Some(Value::Utf8("42".to_string())));

	let resolved = find_identity_by_attribute(&db, "github_user_id", &Value::Utf8("42".to_string()));
	assert_eq!(resolved.map(|ident| ident.id), Some(alice.id));
}
