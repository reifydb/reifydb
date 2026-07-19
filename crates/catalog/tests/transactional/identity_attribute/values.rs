// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::{Value, identity::IdentityId}};
use reifydb_transaction::transaction::Transaction;

fn named_params(entries: &[(&str, Value)]) -> Params {
	let mut map = HashMap::new();
	for (name, value) in entries {
		map.insert(name.to_string(), value.clone());
	}
	Params::from(map)
}

fn value_of(
	catalog: &reifydb_catalog::catalog::Catalog,
	txn: &mut Transaction<'_>,
	username: &str,
	attribute: &str,
) -> Option<String> {
	let identity = catalog.find_identity_by_name(txn, username).unwrap()?;
	let definition = catalog.find_identity_attribute_by_name(txn, attribute).unwrap()?;
	catalog.find_identity_attribute_values(txn, identity.id)
		.unwrap()
		.into_iter()
		.find(|v| v.attribute == definition.id)
		.map(|v| v.value.to_string())
}

#[test]
fn uncommitted_value_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_a: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER iav_alice_a { iav_org_a: 'acme' }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_a", "iav_org_a"),
		Some("acme".to_string()),
		"within-txn attribute value must be visible"
	);
}

#[test]
fn rolled_back_value_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_b: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER iav_alice_b { iav_org_b: 'acme' }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "iav_alice_b").unwrap().is_none(),
		"rolled-back create must remove the identity and its values"
	);
}

#[test]
fn committed_value_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_c: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER iav_alice_c { iav_org_c: 'acme' }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_c", "iav_org_c"),
		Some("acme".to_string()),
		"committed attribute value must be visible in new txn"
	);
}

#[test]
fn uncommitted_value_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_d: utf8");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("CREATE USER iav_alice_d { iav_org_d: 'acme' }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "iav_alice_d").unwrap().is_none(),
		"concurrent txn must not see uncommitted identity or values"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn3), "iav_alice_d", "iav_org_d"),
		Some("acme".to_string()),
		"after commit, attribute value must be visible in a fresh txn"
	);
}

#[test]
fn drop_user_removes_its_values() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_e: utf8");
	t.admin("CREATE USER iav_alice_e { iav_org_e: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_e").unwrap().unwrap();
	let r = txn.rql("DROP USER iav_alice_e", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn2), identity.id).unwrap();
	assert!(values.is_empty(), "dropping a user must cascade its attribute values, found {:?}", values);
}

#[test]
fn drop_attribute_removes_its_values() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_f: utf8");
	t.admin("CREATE USER iav_alice_f { iav_org_f: 'acme' }");

	t.admin("DROP USER ATTRIBUTE iav_org_f");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_f").unwrap().unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn), identity.id).unwrap();
	assert!(values.is_empty(), "dropping an attribute must cascade its values, found {:?}", values);
}

#[test]
fn undeclared_attribute_key_is_rejected() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER iav_dave { iav_undeclared: 'x' }", Params::None);
	let error = r.error.expect("undeclared attribute key must be rejected");
	assert_eq!(error.diagnostic().code, "CA_091");
	// The failed statement poisons the transaction, so absence is asserted in a fresh one.
	drop(txn);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "iav_dave").unwrap().is_none(),
		"rejected create must not leave the identity behind"
	);
}

#[test]
fn duplicate_attribute_key_in_body_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE iav_org_g: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER iav_erin { iav_org_g: 'acme'; iav_org_g: 'globex' }", Params::None);
	let error = r.error.expect("duplicate attribute key must be rejected");
	assert_eq!(error.diagnostic().code, "CA_090");
}

#[test]
fn removed_value_is_gone_and_fails_closed() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_j: utf8");
	t.admin("CREATE USER iav_alice_j { iav_org_j: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_j").unwrap().unwrap();
	let definition = catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "iav_org_j")
		.unwrap()
		.unwrap();
	catalog.remove_identity_attribute_value(&mut txn, identity.id, definition.id).unwrap();

	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_j", "iav_org_j"),
		None,
		"within-txn removed value must not be visible"
	);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_j", "iav_org_j"),
		None,
		"committed removal must persist"
	);
}

#[test]
fn value_created_then_removed_in_one_txn_is_absent_from_overlay() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_m: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER iav_alice_m { iav_org_m: 'acme' }", Params::None);
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_m").unwrap().unwrap();
	let definition = catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "iav_org_m")
		.unwrap()
		.unwrap();
	catalog.remove_identity_attribute_value(&mut txn, identity.id, definition.id).unwrap();

	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn), identity.id).unwrap();
	assert!(
		values.is_empty(),
		"a value created then removed in the same txn must not survive in the overlay, found {:?}",
		values
	);
}

#[test]
fn rolled_back_value_removal_restores_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_k: utf8");
	t.admin("CREATE USER iav_alice_k { iav_org_k: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_k").unwrap().unwrap();
	let definition = catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "iav_org_k")
		.unwrap()
		.unwrap();
	catalog.remove_identity_attribute_value(&mut txn, identity.id, definition.id).unwrap();
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_k", "iav_org_k"),
		Some("acme".to_string()),
		"rolled-back removal must leave the value intact"
	);
}

#[test]
fn declare_and_use_in_same_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER ATTRIBUTE iav_org_i: utf8", Params::None);
	assert!(r.error.is_none(), "declare failed: {:?}", r.error);
	let r = txn.rql("CREATE USER iav_alice_i { iav_org_i: 'acme' }", Params::None);
	assert!(r.error.is_none(), "create with same-txn declared attribute failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_i", "iav_org_i"),
		Some("acme".to_string()),
		"declare + use in one txn must survive commit"
	);
}

// ALTER USER assigns declared attribute values to an already-existing user. The tests
// below cover the full MVCC visibility matrix for assignment and overwrite, since this
// is the first path that writes over a previously committed value.

#[test]
fn alter_user_assigns_value_to_existing_user() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_n: utf8");
	t.admin("CREATE USER iav_alice_n");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("ALTER USER iav_alice_n { iav_org_n: 'acme' }", Params::None);
	assert!(r.error.is_none(), "alter failed: {:?}", r.error);

	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_n", "iav_org_n"),
		Some("acme".to_string()),
		"within-txn assigned value must be visible"
	);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_n", "iav_org_n"),
		Some("acme".to_string()),
		"committed assignment must be visible in new txn"
	);
}

#[test]
fn uncommitted_overwrite_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_o: utf8");
	t.admin("CREATE USER iav_alice_o { iav_org_o: 'acme' }");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("ALTER USER iav_alice_o { iav_org_o: 'globex' }", Params::None);
	assert!(r.error.is_none(), "alter failed: {:?}", r.error);
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn1), "iav_alice_o", "iav_org_o"),
		Some("globex".to_string()),
		"overwriting txn must see its own new value"
	);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_o", "iav_org_o"),
		Some("acme".to_string()),
		"concurrent txn must still see the committed value"
	);
}

#[test]
fn rolled_back_overwrite_restores_old_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_p: utf8");
	t.admin("CREATE USER iav_alice_p { iav_org_p: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("ALTER USER iav_alice_p { iav_org_p: 'globex' }", Params::None);
	assert!(r.error.is_none(), "alter failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_p", "iav_org_p"),
		Some("acme".to_string()),
		"rolled-back overwrite must leave the committed value intact"
	);
}

#[test]
fn committed_overwrite_supersedes_old_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_q: utf8");
	t.admin("CREATE USER iav_alice_q { iav_org_q: 'acme' }");
	t.admin("ALTER USER iav_alice_q { iav_org_q: 'globex' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_q").unwrap().unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn), identity.id).unwrap();
	// Exactly one row must remain: the overwrite must supersede, not duplicate.
	assert_eq!(values.len(), 1, "overwrite must not duplicate the value row, found {:?}", values);
	assert_eq!(
		values[0].value,
		Value::Utf8("globex".to_string()),
		"committed overwrite must supersede the old value"
	);
}

#[test]
fn alter_user_sets_multiple_attributes_in_one_statement() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_r: utf8");
	t.admin("CREATE USER ATTRIBUTE iav_tier_r: utf8");
	t.admin("CREATE USER iav_alice_r { iav_org_r: 'acme' }");
	t.admin("ALTER USER iav_alice_r { iav_org_r: 'globex'; iav_tier_r: 'pro' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_r", "iav_org_r"),
		Some("globex".to_string()),
		"first assignment must overwrite"
	);
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_r", "iav_tier_r"),
		Some("pro".to_string()),
		"second assignment must set the previously unset attribute"
	);
}

#[test]
fn alter_user_leaves_unlisted_attributes_untouched() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_s: utf8");
	t.admin("CREATE USER ATTRIBUTE iav_tier_s: utf8");
	t.admin("CREATE USER iav_alice_s { iav_org_s: 'acme'; iav_tier_s: 'pro' }");
	t.admin("ALTER USER iav_alice_s { iav_tier_s: 'enterprise' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_s", "iav_org_s"),
		Some("acme".to_string()),
		"attributes not listed in the ALTER body must keep their value"
	);
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_s", "iav_tier_s"),
		Some("enterprise".to_string()),
		"listed attribute must be updated"
	);
}

#[test]
fn alter_user_on_nonexistent_user_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE iav_org_t: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("ALTER USER iav_ghost_t { iav_org_t: 'acme' }", Params::None);
	let error = r.error.expect("altering a nonexistent user must be rejected");
	assert_eq!(error.diagnostic().code, "CA_043");
}

#[test]
fn alter_user_with_undeclared_attribute_is_rejected_and_writes_nothing() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_u: utf8");
	t.admin("CREATE USER iav_alice_u { iav_org_u: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	// The declared key comes first: the statement must be all-or-nothing, so even the
	// valid leading assignment must not stick when a later key is undeclared.
	let r = txn.rql("ALTER USER iav_alice_u { iav_org_u: 'globex'; iav_undeclared_u: 'x' }", Params::None);
	let error = r.error.expect("undeclared attribute key must be rejected");
	assert_eq!(error.diagnostic().code, "CA_091");
	drop(txn);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_u", "iav_org_u"),
		Some("acme".to_string()),
		"a rejected ALTER USER must not apply any of its assignments"
	);
}

#[test]
fn alter_user_with_duplicate_key_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE iav_org_v: utf8");
	t.admin("CREATE USER iav_alice_v");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("ALTER USER iav_alice_v { iav_org_v: 'acme'; iav_org_v: 'globex' }", Params::None);
	let error = r.error.expect("duplicate attribute key must be rejected");
	assert_eq!(error.diagnostic().code, "CA_090");
}

#[test]
fn declare_and_alter_in_same_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER iav_alice_w");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER ATTRIBUTE iav_org_w: utf8", Params::None);
	assert!(r.error.is_none(), "declare failed: {:?}", r.error);
	let r = txn.rql("ALTER USER iav_alice_w { iav_org_w: 'acme' }", Params::None);
	assert!(r.error.is_none(), "alter with same-txn declared attribute failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_w", "iav_org_w"),
		Some("acme".to_string()),
		"declare + alter in one txn must survive commit"
	);
}

// CALL identity::set_attribute / remove_attribute are the programmatic API where the user
// arrives as data (IdentityId or name). They route through the same tracked facade as the
// DDL statements, so the tests below prove txn participation plus the argument contract.

#[test]
fn call_set_attribute_assigns_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_aa: utf8");
	t.admin("CREATE USER iav_alice_aa");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CALL identity::set_attribute('iav_alice_aa', 'iav_org_aa', 'acme')", Params::None);
	assert!(r.error.is_none(), "call failed: {:?}", r.error);

	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_aa", "iav_org_aa"),
		Some("acme".to_string()),
		"within-txn CALL-assigned value must be visible"
	);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_aa", "iav_org_aa"),
		Some("acme".to_string()),
		"committed CALL-assigned value must be visible in new txn"
	);
}

#[test]
fn call_set_attribute_by_id() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_ab: utf8");
	t.admin("CREATE USER iav_alice_ab");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_ab").unwrap().unwrap();
	let r = txn.rql(
		"CALL identity::set_attribute($uid, 'iav_org_ab', 'acme')",
		named_params(&[("uid", Value::IdentityId(identity.id))]),
	);
	assert!(r.error.is_none(), "call by id failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_ab", "iav_org_ab"),
		Some("acme".to_string()),
		"value assigned by IdentityId must land on the right user"
	);
}

#[test]
fn call_set_attribute_overwrites_committed_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_ac: utf8");
	t.admin("CREATE USER iav_alice_ac { iav_org_ac: 'acme' }");
	t.admin("CALL identity::set_attribute('iav_alice_ac', 'iav_org_ac', 'globex')");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_ac").unwrap().unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn), identity.id).unwrap();
	assert_eq!(values.len(), 1, "CALL overwrite must not duplicate the value row, found {:?}", values);
	assert_eq!(values[0].value, Value::Utf8("globex".to_string()), "CALL overwrite must supersede the old value");
}

#[test]
fn rolled_back_call_set_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_ad: utf8");
	t.admin("CREATE USER iav_alice_ad { iav_org_ad: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CALL identity::set_attribute('iav_alice_ad', 'iav_org_ad', 'globex')", Params::None);
	assert!(r.error.is_none(), "call failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_ad", "iav_org_ad"),
		Some("acme".to_string()),
		"rolled-back CALL set must leave the committed value intact"
	);
}

#[test]
fn call_remove_attribute_unsets_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_ae: utf8");
	t.admin("CREATE USER iav_alice_ae { iav_org_ae: 'acme' }");
	t.admin("CALL identity::remove_attribute('iav_alice_ae', 'iav_org_ae')");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_ae", "iav_org_ae"),
		None,
		"CALL remove must unset the value (fails closed afterwards)"
	);
}

#[test]
fn call_remove_unset_attribute_is_noop() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE iav_org_af: utf8");
	t.admin("CREATE USER iav_alice_af");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CALL identity::remove_attribute('iav_alice_af', 'iav_org_af')", Params::None);
	assert!(r.error.is_none(), "removing an unset attribute must be a no-op success: {:?}", r.error);
}

#[test]
fn call_set_unknown_attribute_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER iav_alice_ag");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CALL identity::set_attribute('iav_alice_ag', 'iav_undeclared_ag', 'x')", Params::None);
	let error = r.error.expect("undeclared attribute must be rejected");
	let diagnostic = error.diagnostic();
	assert_eq!(diagnostic.code, "PROCEDURE_003");
	assert_eq!(diagnostic.cause.as_ref().expect("wrapped catalog cause").code, "CA_091");
}

#[test]
fn call_set_unknown_user_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE iav_org_ah: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CALL identity::set_attribute('iav_ghost_ah', 'iav_org_ah', 'x')", Params::None);
	let error = r.error.expect("unknown user must be rejected");
	let diagnostic = error.diagnostic();
	assert_eq!(diagnostic.code, "PROCEDURE_003");
	assert_eq!(diagnostic.cause.as_ref().expect("wrapped catalog cause").code, "CA_043");
}

// DDL body values are evaluated expressions since round 3: statement params bind and
// non-utf8 results fail loud instead of storing raw token text (finding #2).

#[test]
fn create_user_with_param_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_ai: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE USER iav_alice_ai { iav_org_ai: $new_org }",
		named_params(&[("new_org", Value::Utf8("acme".to_string()))]),
	);
	assert!(r.error.is_none(), "create with param value failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_ai", "iav_org_ai"),
		Some("acme".to_string()),
		"param-bound body value must be stored"
	);
}

#[test]
fn alter_user_with_param_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_aj: utf8");
	t.admin("CREATE USER iav_alice_aj { iav_org_aj: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"ALTER USER iav_alice_aj { iav_org_aj: $new_org }",
		named_params(&[("new_org", Value::Utf8("globex".to_string()))]),
	);
	assert!(r.error.is_none(), "alter with param value failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_aj", "iav_org_aj"),
		Some("globex".to_string()),
		"param-bound ALTER USER value must overwrite"
	);
}

// Body values are cast to the attribute's declared catalog type with the same house rules
// as INSERT: castable literals convert, uncastable ones raise the cast diagnostic.
#[test]
fn int_body_value_casts_to_declared_utf8_type() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_ak: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER iav_alice_ak { iav_org_ak: 123 }", Params::None);
	assert!(r.error.is_none(), "castable body value must be accepted: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn2), "iav_alice_ak", "iav_org_ak"),
		Some("123".to_string()),
		"123 into a utf8 attribute must store the cast string"
	);
}

#[test]
fn uncastable_body_value_is_rejected() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_rank_am: int4");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER iav_alice_am { iav_rank_am: 'not_a_number' }", Params::None);
	assert!(r.error.is_some(), "an uncastable body value must be rejected");
	drop(txn);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "iav_alice_am").unwrap().is_none(),
		"rejected create must not leave the identity behind"
	);
}

#[test]
fn typed_int_attribute_via_ddl_literal_and_call() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_rank_an: int4");
	t.admin("CREATE USER iav_alice_an { iav_rank_an: 3 }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_an").unwrap().unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn), identity.id).unwrap();
	assert_eq!(values[0].value, Value::Int4(3), "int literal must coerce to the declared int4");
	drop(txn);

	// CALL with an int literal coerces to the declared int4 through the same cast rules.
	t.admin("CALL identity::set_attribute('iav_alice_an', 'iav_rank_an', 7)");
	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn2), identity.id).unwrap();
	assert_eq!(values.len(), 1, "overwrite must supersede, found {:?}", values);
	assert_eq!(values[0].value, Value::Int4(7));
}

#[test]
fn typed_int_attribute_via_named_param() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_rank_ao: int4");
	t.admin("CREATE USER iav_alice_ao");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("ALTER USER iav_alice_ao { iav_rank_ao: $rank }", named_params(&[("rank", Value::Int4(9))]));
	assert!(r.error.is_none(), "typed param must bind: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "iav_alice_ao").unwrap().unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn2), identity.id).unwrap();
	assert_eq!(values[0].value, Value::Int4(9));
}

#[test]
fn typed_bool_attribute_roundtrip() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_admin_ap: bool");
	t.admin("CREATE USER iav_alice_ap { iav_admin_ap: true }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "iav_alice_ap").unwrap().unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn), identity.id).unwrap();
	assert_eq!(values[0].value, Value::Boolean(true));
}

#[test]
fn typed_overwrite_survives_commit_and_rollback() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_rank_aq: int4");
	t.admin("CREATE USER iav_alice_aq { iav_rank_aq: 1 }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("ALTER USER iav_alice_aq { iav_rank_aq: 2 }", Params::None);
	assert!(r.error.is_none(), "typed overwrite failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let identity =
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "iav_alice_aq").unwrap().unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn2), identity.id).unwrap();
	assert_eq!(values[0].value, Value::Int4(1), "rolled-back typed overwrite must restore the old value");
	drop(txn2);

	t.admin("ALTER USER iav_alice_aq { iav_rank_aq: 2 }");
	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn3), identity.id).unwrap();
	assert_eq!(values[0].value, Value::Int4(2), "committed typed overwrite must persist");
}

#[test]
fn option_attribute_declaration_is_rejected() {
	let t = TestEngine::new();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER ATTRIBUTE iav_opt_ar: option(int4)", Params::None);
	let error = r.error.expect("option attribute types must be rejected");
	assert_eq!(error.diagnostic().code, "CA_092");
}

#[test]
fn none_param_body_value_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE iav_org_al: utf8");
	t.admin("CREATE USER iav_alice_al { iav_org_al: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn
		.rql("ALTER USER iav_alice_al { iav_org_al: $new_org }", named_params(&[("new_org", Value::none())]));
	let error = r.error.expect("none body value must be rejected, not silently unset");
	assert_eq!(error.diagnostic().code, "CA_094");
}

#[test]
fn drop_and_redeclare_does_not_resurrect_old_value() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iav_org_h: utf8");
	t.admin("CREATE USER iav_alice_h { iav_org_h: 'acme' }");
	t.admin("DROP USER ATTRIBUTE iav_org_h");
	t.admin("CREATE USER ATTRIBUTE iav_org_h: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	assert_eq!(
		value_of(&catalog, &mut Transaction::Admin(&mut txn), "iav_alice_h", "iav_org_h"),
		None,
		"a redeclared attribute must not rebind values set under the dropped definition"
	);
}
