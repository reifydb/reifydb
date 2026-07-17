// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Verifies that table `from` policies scope subscription LIVE DIFFS per subscriber identity.
// Regression guard: subscription flow compilation previously skipped inject_from_policies and
// evaluated flow filters with an empty symbol table as root, so any authenticated subscriber
// received every tenant's change events even though hydration was correctly scoped.
//
// Also covers subscription parameters: $name references in the subscription body must resolve
// at change time from the params captured when the subscription was created.

use std::{collections::HashMap, sync::Arc};

use reifydb::{Database, Params, embedded as db_embedded};
use reifydb_core::value::column::columns::Columns;
use reifydb_value::value::{Value, identity::IdentityId};

use crate::common::{drain_after_consumer_caught_up, extract_sub_id};

fn lookup_identity(db: &Database, name: &str) -> IdentityId {
	let frames = db
		.query_as_root(&format!("from system::identities filter {{ name == '{name}' }}"), Params::None)
		.expect("identity lookup");
	let frame = frames.first().expect("identity frame");
	let col = frame.columns.iter().find(|c| c.name == "id").expect("id column");
	match col.data.get_value(0) {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value: {other:?}"),
	}
}

fn setup() -> (Database, IdentityId, IdentityId) {
	let db = db_embedded::memory().build().expect("build");
	db.admin_as_root("create namespace app", Params::None).expect("namespace");
	db.admin_as_root("create table app::docs { owner_id: identity_id, content: utf8 }", Params::None)
		.expect("table");
	db.admin_as_root("create user alice", Params::None).expect("alice");
	db.admin_as_root("create user bob", Params::None).expect("bob");
	db.admin_as_root("create session policy allow_subscribe { subscription: { filter { true } } }", Params::None)
		.expect("session policy");
	db.admin_as_root(
		"create table policy docs_owner on app::docs { from: { filter { owner_id == $identity.id } } }",
		Params::None,
	)
	.expect("from policy");
	let alice = lookup_identity(&db, "alice");
	let bob = lookup_identity(&db, "bob");
	(db, alice, bob)
}

fn insert_docs(db: &Database, alice: IdentityId, bob: IdentityId) {
	db.command_as_root(
		&format!("insert app::docs [\
			 {{ owner_id: cast('{alice}', identity_id), content: 'alice-doc' }}, \
			 {{ owner_id: cast('{bob}', identity_id), content: 'bob-doc' }}]"),
		Params::None,
	)
	.expect("insert docs");
}

fn contents(batches: &[Columns]) -> Vec<String> {
	let mut out = Vec::new();
	for cols in batches {
		let Some(content) = cols.iter().find(|c| c.name().text() == "content") else {
			continue;
		};
		for i in 0..cols.row_count() {
			match content.data().get_value(i) {
				Value::Utf8(s) => out.push(s),
				other => panic!("unexpected content value: {other:?}"),
			}
		}
	}
	out.sort();
	out
}

// The core regression: alice subscribes to the raw table WITHOUT any filter of her own; the
// injected from-policy must keep bob's rows out of her live diffs. Before the fix this test
// would observe both 'alice-doc' and 'bob-doc'.
#[test]
fn from_policy_scopes_live_diffs_per_subscriber() {
	let (db, alice, bob) = setup();

	let result = db.engine().subscribe_as(alice, "create subscription as { from app::docs }", Params::None);
	assert!(result.error.is_none(), "subscribe as alice failed: {:?}", result.error);
	let sub_id = extract_sub_id(&result.frames);

	insert_docs(&db, alice, bob);

	let batches = drain_after_consumer_caught_up(&db, sub_id);
	assert_eq!(
		contents(&batches),
		vec!["alice-doc".to_string()],
		"alice's subscription must only ever deliver her own rows"
	);
}

// Root bypasses policies entirely; a root subscription over the same policed table must keep
// seeing every tenant's diffs. Guards against the fix over-applying default-deny to root.
#[test]
fn root_subscription_bypasses_policies() {
	let (db, alice, bob) = setup();

	let result =
		db.engine().subscribe_as(IdentityId::root(), "create subscription as { from app::docs }", Params::None);
	assert!(result.error.is_none(), "subscribe as root failed: {:?}", result.error);
	let sub_id = extract_sub_id(&result.frames);

	insert_docs(&db, alice, bob);

	let batches = drain_after_consumer_caught_up(&db, sub_id);
	assert_eq!(
		contents(&batches),
		vec!["alice-doc".to_string(), "bob-doc".to_string()],
		"root must see all tenants' rows"
	);
}

// A table with policies but a subscriber whose rows never match must receive ZERO diffs while
// other tenants' changes stream. This is the leak scenario observed in production spikes.
#[test]
fn non_matching_subscriber_receives_no_diffs() {
	let (db, alice, bob) = setup();

	let result = db.engine().subscribe_as(bob, "create subscription as { from app::docs }", Params::None);
	assert!(result.error.is_none(), "subscribe as bob failed: {:?}", result.error);
	let sub_id = extract_sub_id(&result.frames);

	db.command_as_root(
		&format!("insert app::docs [{{ owner_id: cast('{alice}', identity_id), content: 'alice-doc' }}]"),
		Params::None,
	)
	.expect("insert alice doc");

	let batches = drain_after_consumer_caught_up(&db, sub_id);
	assert_eq!(contents(&batches), Vec::<String>::new(), "bob must not receive alice's diffs");
}

// Subscription params: a $name reference in the body resolves from the params the subscription
// was created with, at change time. Uses root so policy injection stays out of the picture and
// the assertion isolates parameter resolution in the flow operators.
#[test]
fn subscription_params_resolve_in_flow_filters() {
	let (db, alice, bob) = setup();

	let mut named = HashMap::new();
	named.insert("wanted".to_string(), Value::Utf8("bob-doc".to_string()));
	let result = db.engine().subscribe_as(
		IdentityId::root(),
		"create subscription as { from app::docs filter { content == $wanted } }",
		Params::Named(Arc::new(named)),
	);
	assert!(result.error.is_none(), "subscribe with params failed: {:?}", result.error);
	let sub_id = extract_sub_id(&result.frames);

	insert_docs(&db, alice, bob);

	let batches = drain_after_consumer_caught_up(&db, sub_id);
	assert_eq!(
		contents(&batches),
		vec!["bob-doc".to_string()],
		"only rows matching the $wanted param must be delivered"
	);
}
