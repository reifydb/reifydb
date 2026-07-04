// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Direct-execute tests for identity::set_attribute / identity::remove_attribute: the full
// argument contract (arity, per-position types, none), the admin/privileged gate, resolution
// by name and by IdentityId, catalog error propagation, and the result column shape. These
// call Routine::execute directly so RoutineError variants can be asserted precisely, which
// the rql/CALL integration layer wraps and hides.

use reifydb_core::value::column::columns::Columns;
use reifydb_engine::test_prelude::*;
use reifydb_routine::{
	procedure::identity::{remove_attribute::RemoveIdentityAttribute, set_attribute::SetIdentityAttribute},
	routine::{Routine, context::ProcedureContext, error::RoutineError},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

fn run_set(
	t: &TestEngine,
	tx: &mut Transaction<'_>,
	identity: IdentityId,
	args: Vec<Value>,
) -> Result<Columns, RoutineError> {
	let services = t.services();
	let catalog = t.catalog();
	let params = Params::from(args);
	let mut ctx = ProcedureContext {
		fragment: Fragment::internal("identity::set_attribute"),
		identity,
		row_count: 1,
		runtime_context: &services.runtime_context,
		tx,
		params: &params,
		catalog: &catalog,
		ioc: t.ioc(),
	};
	SetIdentityAttribute::new().execute(&mut ctx, &Columns::empty())
}

fn run_remove(
	t: &TestEngine,
	tx: &mut Transaction<'_>,
	identity: IdentityId,
	args: Vec<Value>,
) -> Result<Columns, RoutineError> {
	let services = t.services();
	let catalog = t.catalog();
	let params = Params::from(args);
	let mut ctx = ProcedureContext {
		fragment: Fragment::internal("identity::remove_attribute"),
		identity,
		row_count: 1,
		runtime_context: &services.runtime_context,
		tx,
		params: &params,
		catalog: &catalog,
		ioc: t.ioc(),
	};
	RemoveIdentityAttribute::new().execute(&mut ctx, &Columns::empty())
}

fn column_value(columns: &Columns, index: usize) -> Value {
	columns.columns[index].get_value(0)
}

fn stored_value(t: &TestEngine, tx: &mut Transaction<'_>, user: &str, attribute: &str) -> Option<String> {
	let catalog = t.catalog();
	let identity = catalog.find_identity_by_name(tx, user).unwrap()?;
	let definition = catalog.find_identity_attribute_by_name(tx, attribute).unwrap()?;
	catalog.find_identity_attribute_values(tx, identity.id)
		.unwrap()
		.into_iter()
		.find(|v| v.attribute == definition.id)
		.map(|v| v.value.to_string())
}

fn utf8(s: &str) -> Value {
	Value::Utf8(s.to_string())
}

#[test]
fn set_with_no_args_is_arity_mismatch() {
	let t = TestEngine::new();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(&t, &mut Transaction::Admin(&mut txn), IdentityId::system(), vec![]).unwrap_err();
	match err {
		RoutineError::ProcedureArityMismatch {
			expected,
			actual,
			..
		} => {
			assert_eq!(expected, 3);
			assert_eq!(actual, 0);
		}
		other => panic!("expected arity mismatch, got {other:?}"),
	}
}

#[test]
fn set_with_two_args_is_arity_mismatch() {
	let t = TestEngine::new();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("alice"), utf8("org_id")],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureArityMismatch {
			expected,
			actual,
			..
		} => {
			assert_eq!(expected, 3);
			assert_eq!(actual, 2);
		}
		other => panic!("expected arity mismatch, got {other:?}"),
	}
}

#[test]
fn set_with_four_args_is_arity_mismatch() {
	let t = TestEngine::new();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("alice"), utf8("org_id"), utf8("acme"), utf8("extra")],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureArityMismatch {
			actual,
			..
		} => assert_eq!(actual, 4),
		other => panic!("expected arity mismatch, got {other:?}"),
	}
}

#[test]
fn set_with_non_identity_user_arg_is_type_error() {
	let t = TestEngine::new();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![Value::Int4(7), utf8("org_id"), utf8("acme")],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureInvalidArgumentType {
			argument_index,
			expected,
			actual,
			..
		} => {
			assert_eq!(argument_index, 0);
			assert_eq!(expected, vec![ValueType::IdentityId, ValueType::Utf8]);
			assert_eq!(actual, ValueType::Int4);
		}
		other => panic!("expected invalid argument type, got {other:?}"),
	}
}

#[test]
fn set_with_non_utf8_attribute_arg_is_type_error() {
	let t = TestEngine::new();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("alice"), Value::Int4(7), utf8("acme")],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureInvalidArgumentType {
			argument_index,
			..
		} => assert_eq!(argument_index, 1),
		other => panic!("expected invalid argument type, got {other:?}"),
	}
}

// Values are cast to the attribute's declared catalog type with the same house rules as
// INSERT: castable values convert (bool -> utf8 stores "true"), uncastable ones raise the
// cast diagnostic, and none is rejected before casting.
#[test]
fn set_bool_value_into_utf8_attribute_casts() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_o: utf8");
	t.admin("CREATE USER rp_alice_o");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_o"), utf8("rp_org_o"), Value::Boolean(true)],
	)
	.unwrap();
	assert_eq!(
		stored_value(&t, &mut Transaction::Admin(&mut txn), "rp_alice_o", "rp_org_o"),
		Some("true".to_string()),
		"castable values must convert to the declared type like INSERT does"
	);
}

#[test]
fn set_uncastable_value_is_cast_error() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_rank_p: int4");
	t.admin("CREATE USER rp_alice_p");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_p"), utf8("rp_rank_p"), utf8("not_a_number")],
	)
	.unwrap_err();
	match err {
		RoutineError::Wrapped(e) => {
			let code = e.diagnostic().code.clone();
			assert!(!code.is_empty(), "cast failure must carry a diagnostic, got {code}");
		}
		other => panic!("expected wrapped cast error, got {other:?}"),
	}
	assert_eq!(
		stored_value(&t, &mut Transaction::Admin(&mut txn), "rp_alice_p", "rp_rank_p"),
		None,
		"a failed cast must not store anything"
	);
}

#[test]
fn set_with_none_value_arg_is_type_error() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_q: utf8");
	t.admin("CREATE USER rp_alice_q");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_q"), utf8("rp_org_q"), Value::none()],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureInvalidArgumentType {
			argument_index,
			expected,
			..
		} => {
			assert_eq!(argument_index, 2);
			assert_eq!(expected, vec![ValueType::Utf8]);
		}
		other => panic!("expected invalid argument type for none, got {other:?}"),
	}
}

#[test]
fn set_by_name_stores_value_and_reports_columns() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_a: utf8");
	t.admin("CREATE USER rp_alice_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let columns = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_a"), utf8("rp_org_a"), utf8("acme")],
	)
	.unwrap();
	assert_eq!(column_value(&columns, 0), utf8("rp_alice_a"));
	assert_eq!(column_value(&columns, 1), utf8("rp_org_a"));
	assert_eq!(column_value(&columns, 2), utf8("acme"));

	assert_eq!(
		stored_value(&t, &mut Transaction::Admin(&mut txn), "rp_alice_a", "rp_org_a"),
		Some("acme".to_string()),
		"the value must be stored through the tracked facade"
	);
}

#[test]
fn set_by_identity_id_stores_value() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_b: utf8");
	t.admin("CREATE USER rp_alice_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let catalog = t.catalog();
	let identity = catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "rp_alice_b").unwrap().unwrap();
	let columns = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![Value::IdentityId(identity.id), utf8("rp_org_b"), utf8("acme")],
	)
	.unwrap();
	// The result echoes the resolved NAME, never the uuid.
	assert_eq!(column_value(&columns, 0), utf8("rp_alice_b"));

	assert_eq!(
		stored_value(&t, &mut Transaction::Admin(&mut txn), "rp_alice_b", "rp_org_b"),
		Some("acme".to_string()),
		"assignment addressed by id must land on the right user"
	);
}

#[test]
fn set_overwrites_previous_value() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_c: utf8");
	t.admin("CREATE USER rp_alice_c { rp_org_c: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_c"), utf8("rp_org_c"), utf8("globex")],
	)
	.unwrap();

	let catalog = t.catalog();
	let identity = catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "rp_alice_c").unwrap().unwrap();
	let values = catalog.find_identity_attribute_values(&mut Transaction::Admin(&mut txn), identity.id).unwrap();
	assert_eq!(values.len(), 1, "overwrite must supersede, not duplicate, found {values:?}");
	assert_eq!(values[0].value, Value::Utf8("globex".to_string()));
}

#[test]
fn set_unknown_user_by_name_is_ca_043() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_d: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_ghost_d"), utf8("rp_org_d"), utf8("acme")],
	)
	.unwrap_err();
	match err {
		RoutineError::Wrapped(e) => assert_eq!(e.diagnostic().code, "CA_043"),
		other => panic!("expected wrapped CA_043, got {other:?}"),
	}
}

#[test]
fn set_unknown_user_by_id_is_ca_043() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_e: utf8");
	t.admin("CREATE USER rp_temp_e");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let catalog = t.catalog();
	let stale = catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "rp_temp_e").unwrap().unwrap().id;
	drop(txn);
	t.admin("DROP USER rp_temp_e");

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn2),
		IdentityId::system(),
		vec![Value::IdentityId(stale), utf8("rp_org_e"), utf8("acme")],
	)
	.unwrap_err();
	match err {
		RoutineError::Wrapped(e) => assert_eq!(e.diagnostic().code, "CA_043"),
		other => panic!("expected wrapped CA_043 for stale id, got {other:?}"),
	}
}

#[test]
fn set_undeclared_attribute_is_ca_091() {
	let t = TestEngine::new();
	t.admin("CREATE USER rp_alice_f");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_f"), utf8("rp_undeclared_f"), utf8("acme")],
	)
	.unwrap_err();
	match err {
		RoutineError::Wrapped(e) => assert_eq!(e.diagnostic().code, "CA_091"),
		other => panic!("expected wrapped CA_091, got {other:?}"),
	}
}

#[test]
fn set_on_command_transaction_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_g: utf8");
	t.admin("CREATE USER rp_alice_g");

	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Command(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_g"), utf8("rp_org_g"), utf8("acme")],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureExecutionFailed {
			reason,
			..
		} => assert!(reason.contains("admin transaction"), "unexpected reason: {reason}"),
		other => panic!("expected execution failed on command txn, got {other:?}"),
	}
}

#[test]
fn set_on_query_transaction_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_h: utf8");
	t.admin("CREATE USER rp_alice_h");

	let mut txn = t.begin_query(IdentityId::system()).unwrap();
	let err = run_set(
		&t,
		&mut Transaction::Query(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_h"), utf8("rp_org_h"), utf8("acme")],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureExecutionFailed {
			..
		} => {}
		other => panic!("expected execution failed on query txn, got {other:?}"),
	}
}

#[test]
fn remove_with_one_arg_is_arity_mismatch() {
	let t = TestEngine::new();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_remove(&t, &mut Transaction::Admin(&mut txn), IdentityId::system(), vec![utf8("alice")])
		.unwrap_err();
	match err {
		RoutineError::ProcedureArityMismatch {
			expected,
			actual,
			..
		} => {
			assert_eq!(expected, 2);
			assert_eq!(actual, 1);
		}
		other => panic!("expected arity mismatch, got {other:?}"),
	}
}

#[test]
fn remove_with_non_utf8_attribute_arg_is_type_error() {
	let t = TestEngine::new();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_remove(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("alice"), Value::Int4(7)],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureInvalidArgumentType {
			argument_index,
			..
		} => assert_eq!(argument_index, 1),
		other => panic!("expected invalid argument type, got {other:?}"),
	}
}

#[test]
fn remove_unsets_value_and_reports_columns() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_i: utf8");
	t.admin("CREATE USER rp_alice_i { rp_org_i: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let columns = run_remove(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_i"), utf8("rp_org_i")],
	)
	.unwrap();
	assert_eq!(column_value(&columns, 0), utf8("rp_alice_i"));
	assert_eq!(column_value(&columns, 1), utf8("rp_org_i"));
	assert_eq!(column_value(&columns, 2), Value::Boolean(true));

	assert_eq!(
		stored_value(&t, &mut Transaction::Admin(&mut txn), "rp_alice_i", "rp_org_i"),
		None,
		"removal must unset the value"
	);
}

#[test]
fn remove_by_identity_id_unsets_value() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_j: utf8");
	t.admin("CREATE USER rp_alice_j { rp_org_j: 'acme' }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let catalog = t.catalog();
	let identity = catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "rp_alice_j").unwrap().unwrap();
	run_remove(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![Value::IdentityId(identity.id), utf8("rp_org_j")],
	)
	.unwrap();

	assert_eq!(stored_value(&t, &mut Transaction::Admin(&mut txn), "rp_alice_j", "rp_org_j"), None);
}

#[test]
fn remove_unset_attribute_is_noop_success() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_k: utf8");
	t.admin("CREATE USER rp_alice_k");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let columns = run_remove(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_k"), utf8("rp_org_k")],
	)
	.unwrap();
	assert_eq!(column_value(&columns, 2), Value::Boolean(true));
}

#[test]
fn remove_unknown_user_is_ca_043() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_l: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_remove(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_ghost_l"), utf8("rp_org_l")],
	)
	.unwrap_err();
	match err {
		RoutineError::Wrapped(e) => assert_eq!(e.diagnostic().code, "CA_043"),
		other => panic!("expected wrapped CA_043, got {other:?}"),
	}
}

#[test]
fn remove_undeclared_attribute_is_ca_091() {
	let t = TestEngine::new();
	t.admin("CREATE USER rp_alice_m");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = run_remove(
		&t,
		&mut Transaction::Admin(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_m"), utf8("rp_undeclared_m")],
	)
	.unwrap_err();
	match err {
		RoutineError::Wrapped(e) => assert_eq!(e.diagnostic().code, "CA_091"),
		other => panic!("expected wrapped CA_091, got {other:?}"),
	}
}

#[test]
fn remove_on_command_transaction_is_rejected() {
	let t = TestEngine::new();
	t.admin("CREATE USER ATTRIBUTE rp_org_n: utf8");
	t.admin("CREATE USER rp_alice_n { rp_org_n: 'acme' }");

	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let err = run_remove(
		&t,
		&mut Transaction::Command(&mut txn),
		IdentityId::system(),
		vec![utf8("rp_alice_n"), utf8("rp_org_n")],
	)
	.unwrap_err();
	match err {
		RoutineError::ProcedureExecutionFailed {
			reason,
			..
		} => assert!(reason.contains("admin transaction"), "unexpected reason: {reason}"),
		other => panic!("expected execution failed on command txn, got {other:?}"),
	}
}
