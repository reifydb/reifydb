// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ffi::c_void;

use reifydb_core::common::CommitVersion;
use reifydb_sdk::flow::operator::extern_c::{binding::context::ExternCOperatorContext, wire::context::ExternCContext};
use reifydb_testing_sdk::{callbacks::create_test_callbacks, context::TestContext};
use reifydb_value::value::{Value, dictionary::DictionaryId, value_type::ValueType};

#[test]
fn dictionary_round_trips_through_extern_c() {
	let test_ctx = TestContext::new(CommitVersion(1));
	test_ctx.seed_dictionary(
		"solana::mints",
		7,
		ValueType::Uint4,
		&[(1, Value::Utf8("MINTA".to_string())), (2, Value::Utf8("MINTB".to_string()))],
	);

	let mut extern_c_context = ExternCContext {
		txn_ptr: &test_ctx as *const TestContext as *mut c_void,
		written_at_nanos: 0,
		operator_id: 1,
		callbacks: create_test_callbacks(),
	};
	let mut ctx = ExternCOperatorContext::new(&mut extern_c_context as *mut ExternCContext);

	let id = ctx.dictionary().id_by_name("solana::mints").unwrap().expect("dictionary id");
	assert_eq!(id, DictionaryId(7));

	let entry = ctx.dictionary().find(id, &Value::Utf8("MINTA".to_string())).unwrap().expect("entry id");
	let decoded = ctx.dictionary().get(id, entry).unwrap().expect("decoded value");
	assert_eq!(decoded, Value::Utf8("MINTA".to_string()));

	assert!(ctx.dictionary().find(id, &Value::Utf8("MISSING".to_string())).unwrap().is_none());
	assert!(ctx.dictionary().id_by_name("solana::unknown").unwrap().is_none());
}
