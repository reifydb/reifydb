// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::identity::IdentityId};

fn user(t: &TestEngine, name: &str) -> IdentityId {
	// users made by the fixture builder in one millisecond share an id, which voids every ownership check.
	t.admin(&format!("CREATE USER {name}"));
	let frames = t.query(&format!("FROM system::identities FILTER {{ name == '{name}' }} MAP {{ id }}"));
	frames[0].rows().next().expect("the user must exist").get::<IdentityId>("id").unwrap().unwrap()
}

fn owned_rows(kind: &str, create: &str) -> (TestEngine, IdentityId, IdentityId) {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(create);
	t.admin(&format!("CREATE {kind} POLICY own ON test::t {{ from: {{ filter {{ owner == $identity.id }} }}, \
		 update: {{ filter {{ owner == $identity.id }} }}, delete: {{ filter {{ owner == $identity.id }} }} }}"));
	let alice = user(&t, "alice");
	let bob = user(&t, "bob");
	assert_ne!(alice, bob, "the fixture needs two distinct owners");
	t.command(&format!("INSERT test::t [{{ k: 1, owner: cast('{alice}', identity_id), data: 'a' }}, \
		 {{ k: 2, owner: cast('{bob}', identity_id), data: 'b' }}]"));
	(t, alice, bob)
}

fn owned_table() -> (TestEngine, IdentityId, IdentityId) {
	owned_rows("TABLE", "CREATE TABLE test::t { k: int8, owner: identity_id, data: utf8 }")
}

fn owned_ringbuffer() -> (TestEngine, IdentityId, IdentityId) {
	owned_rows(
		"RINGBUFFER",
		"CREATE RINGBUFFER test::t { k: int8, owner: identity_id, data: utf8 } WITH { capacity: 10 }",
	)
}

fn owned_series() -> (TestEngine, IdentityId, IdentityId) {
	owned_rows("SERIES", "CREATE SERIES test::t { k: int8, owner: identity_id, data: utf8 } WITH { key: k }")
}

fn owner_of(t: &TestEngine, k: i64) -> IdentityId {
	let frames = t.query(&format!("FROM test::t FILTER {{ k == {k} }} MAP {{ owner }}"));
	let row = frames[0].rows().next().unwrap_or_else(|| panic!("row {k} is gone"));
	row.get::<IdentityId>("owner").unwrap().unwrap()
}

fn data_of(t: &TestEngine, k: i64) -> String {
	let frames = t.query(&format!("FROM test::t FILTER {{ k == {k} }} MAP {{ data }}"));
	let row = frames[0].rows().next().unwrap_or_else(|| panic!("row {k} is gone"));
	row.get::<String>("data").unwrap().unwrap()
}

fn take_over_accepted(t: &TestEngine, who: IdentityId) -> bool {
	let rql = "UPDATE test::t { owner: $identity.id, data: 'taken' } FILTER { true }";
	t.inner().command_as(who, rql, Params::None).error.is_none()
}

#[test]
fn a_user_cannot_take_over_another_owners_table_row() {
	// the old row must pass the update policy too, otherwise a writer stamps its own id onto every row.
	let (t, alice, bob) = owned_table();
	assert!(!take_over_accepted(&t, alice), "alice's takeover of bob's row was accepted");
	assert_eq!((owner_of(&t, 2), data_of(&t, 2)), (bob, "b".to_string()), "bob's row changed");
	assert_eq!(data_of(&t, 1), "a", "a denied update must leave every row as it was");
}

#[test]
fn a_user_cannot_give_its_own_table_row_away() {
	// the new row must pass the update policy too, otherwise a writer pushes rows onto an owner that never agreed.
	let (t, alice, bob) = owned_table();
	let give = format!("UPDATE test::t {{ owner: cast('{bob}', identity_id) }} FILTER {{ k == 1 }}");
	let r = t.inner().command_as(alice, &give, Params::None);
	assert!(r.error.is_some(), "alice gave her row to bob");
	assert_eq!(owner_of(&t, 1), alice, "alice's row changed owner");
}

#[test]
fn a_user_can_update_its_own_table_row_keeping_ownership() {
	// judging the old row must never deny the ordinary edit where both images belong to the writer.
	let (t, alice, _) = owned_table();
	let r = t.inner().command_as(alice, "UPDATE test::t { data: 'edited' } FILTER { k == 1 }", Params::None);
	assert!(r.error.is_none(), "alice's update of her own row was denied: {:?}", r.error);
	assert_eq!((owner_of(&t, 1), data_of(&t, 1)), (alice, "edited".to_string()), "the edit must keep the owner");
}

#[test]
fn a_user_cannot_delete_another_owners_table_row() {
	// the delete policy must judge the stored row, otherwise a writer removes rows it does not own.
	let (t, alice, bob) = owned_table();
	let r = t.inner().command_as(alice, "DELETE test::t FILTER { k == 2 }", Params::None);
	assert!(r.error.is_some(), "alice deleted bob's row");
	assert_eq!(owner_of(&t, 2), bob, "bob's row must survive a denied delete");
}

#[test]
fn a_user_cannot_take_over_another_owners_ringbuffer_row() {
	// ringbuffer updates enforce policies on their own path, which must judge the old row as well.
	let (t, alice, _) = owned_ringbuffer();
	assert!(!take_over_accepted(&t, alice), "alice's takeover of bob's ringbuffer row was accepted");
	assert_eq!(data_of(&t, 2), "b", "bob's ringbuffer row changed");
}

#[test]
fn a_user_cannot_take_over_another_owners_series_row() {
	// series updates enforce policies on their own path, which must judge the old row as well.
	let (t, alice, _) = owned_series();
	assert!(!take_over_accepted(&t, alice), "alice's takeover of bob's series row was accepted");
	assert_eq!(data_of(&t, 2), "b", "bob's series row changed");
}
